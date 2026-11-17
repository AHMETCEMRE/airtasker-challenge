"""Weather forecast ingestion.

Fetch and land, nothing else. No scoring, no filtering, no aggregation: all of
that belongs in dbt where it is testable and where there is exactly one copy of
it. The only judgement this asset makes is refusing to send coordinates that
cannot be real.
"""

import time
from datetime import datetime, timezone

from dagster import (
    AssetExecutionContext,
    Backoff,
    Jitter,
    MetadataValue,
    RetryPolicy,
    asset,
)
from dagster_duckdb import DuckDBResource

from weather_risk.open_meteo import (
    FORECAST_DAYS,
    HOURLY_VARIABLES,
    ForecastUnavailable,
    Location,
    build_session,
    fetch_forecast,
    forecast_rows,
)

# Column order must match forecast_rows() in open_meteo.py.
FORECAST_TABLE_DDL = """
create schema if not exists raw;

create table if not exists raw.raw_weather_forecast (
    location_id                 varchar   not null,
    latitude                    double,
    longitude                   double,
    timezone_requested          varchar,
    forecast_time_utc           timestamp not null,
    temperature_2m_c            double,
    precipitation_mm            double,
    precipitation_probability   double,
    wind_speed_kmh              double,
    wind_gusts_kmh              double,
    weather_code                integer,
    fetched_at_utc              timestamp not null,
    dagster_run_id              varchar
);
"""

LOCATIONS_QUERY = """
select
    location_id,
    try_cast(lat as double)  as latitude,
    try_cast(lng as double)  as longitude,
    nullif(trim(timezone), '') as timezone_name
from raw.raw_locations
order by location_id
"""

# Open-Meteo is free and unauthenticated, so being a good citizen is on us. At 20
# locations this adds ~2s; at 40,000 it would be replaced by batched coordinate
# requests (see SPEC.md, Production).
INTER_REQUEST_DELAY_SECONDS = 0.1


@asset(
    description=(
        "Hourly weather forecasts from Open-Meteo, one row per location per "
        "forecast hour per fetch. Append-only: every fetch is kept so a task's "
        "risk can be compared across pipeline runs."
    ),
    compute_kind="python",
    group_name="ingestion",
    # Whole-asset retries, distinct from the per-request retries inside the
    # session. This covers a failure that outlives a single HTTP exchange, such
    # as Open-Meteo being down or the DuckDB file being locked by another writer.
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=30,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
)
def raw_weather_forecast(
    context: AssetExecutionContext, duckdb: DuckDBResource
) -> None:
    """Fetch forecasts for every known location and append them to DuckDB.

    Fetches for all locations rather than only those with an upcoming task: the
    forecast table is then independent of task-side business rules, so it stays
    reproducible when those rules change, and no business logic lives in Python.

    A location that fails after retries is logged and skipped rather than failing
    the run. Nineteen cities' warnings are worth more than none, and the failure
    count is published as asset metadata so the gap is visible rather than
    silent. The asset only fails if nothing at all could be fetched.
    """
    # Read the location list and release the connection before any HTTP work:
    # DuckDB allows a single writer, and holding it open across the fetch would
    # block dbt for the whole run.
    with duckdb.get_connection() as conn:
        conn.execute(FORECAST_TABLE_DDL)
        location_records = conn.execute(LOCATIONS_QUERY).fetchall()

    locations = [
        Location(
            location_id=row[0],
            latitude=row[1],
            longitude=row[2],
            timezone_name=row[3],
        )
        for row in location_records
    ]

    if not locations:
        raise RuntimeError(
            "raw.raw_locations is empty - the dbt seed has not run. "
            "Check scripts/entrypoint.sh."
        )

    valid = [loc for loc in locations if loc.has_valid_coordinates]
    invalid = [loc for loc in locations if not loc.has_valid_coordinates]

    for loc in invalid:
        context.log.warning(
            "Skipping %s: implausible coordinates (lat=%s, lng=%s). "
            "Tasks at this location will be scored 'unknown'. "
            "This is a source data bug and should be fixed in raw_locations, "
            "not silently repaired here.",
            loc.location_id,
            loc.latitude,
            loc.longitude,
        )

    fetched_at_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    session = build_session()

    rows: list[tuple] = []
    failures: dict[str, str] = {}

    for i, location in enumerate(valid):
        try:
            payload = fetch_forecast(session, location)
        except ForecastUnavailable as exc:
            failures[location.location_id] = str(exc)
            context.log.error("Forecast fetch failed: %s", exc)
            continue

        location_rows = forecast_rows(
            location=location,
            payload=payload,
            fetched_at_utc=fetched_at_utc,
            dagster_run_id=context.run_id,
        )
        rows.extend(location_rows)
        context.log.info(
            "Fetched %d forecast hours for %s",
            len(location_rows),
            location.location_id,
        )

        if i < len(valid) - 1:
            time.sleep(INTER_REQUEST_DELAY_SECONDS)

    if not rows:
        raise RuntimeError(
            f"No forecasts could be fetched. "
            f"{len(invalid)} location(s) had invalid coordinates and "
            f"{len(failures)} request(s) failed: {failures}"
        )

    with duckdb.get_connection() as conn:
        conn.executemany(
            """
            insert into raw.raw_weather_forecast values
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        total_rows, total_fetches = conn.execute(
            """
            select count(*), count(distinct fetched_at_utc)
            from raw.raw_weather_forecast
            """
        ).fetchone()

    context.add_output_metadata(
        {
            "locations_in_seed": len(locations),
            "locations_fetched": len(valid) - len(failures),
            "locations_failed": len(failures),
            "locations_skipped_invalid_coordinates": len(invalid),
            "skipped_location_ids": MetadataValue.json(
                [loc.location_id for loc in invalid]
            ),
            "failed_location_ids": MetadataValue.json(sorted(failures)),
            "rows_appended": len(rows),
            "rows_in_table_total": total_rows,
            "fetches_retained": total_fetches,
            "forecast_days": FORECAST_DAYS,
            "hourly_variables": MetadataValue.json(list(HOURLY_VARIABLES)),
            "fetched_at_utc": MetadataValue.text(fetched_at_utc.isoformat()),
        }
    )
