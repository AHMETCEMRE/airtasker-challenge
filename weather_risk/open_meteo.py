"""Open-Meteo forecast client.

Pure HTTP and parsing: no Dagster, no DuckDB. Keeping it separate means the retry
behaviour and the payload-to-rows transform can be tested without the stack
running, which is where most of the ingestion risk actually lives.

API docs: https://open-meteo.com/en/docs
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Sequence

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

# Hourly, not daily. A task is scheduled at an hour, and a daily
# precipitation_sum cannot say whether the rain falls at 3am or at 9am. Ops'
# daily summary is aggregated from these same rows in dbt, so Product and
# Operations can never be computed from different numbers.
HOURLY_VARIABLES: tuple[str, ...] = (
    "temperature_2m",
    "precipitation",
    "precipitation_probability",
    "wind_speed_10m",
    "wind_gusts_10m",
    "weather_code",
)

# Assigned tasks are scheduled 1-14 days out. The API default horizon is 7 days,
# which would silently leave roughly half of them with no forecast at all.
FORECAST_DAYS = 16

# Timestamps are requested in UTC and converted to each location's local time
# downstream in dbt, using the IANA name and DuckDB's ICU extension.
#
# Asking the API for a named timezone instead returns one utc_offset_seconds for
# the whole response, and a single offset cannot describe a 16-day window that
# contains a DST transition - every hour after the transition would be shifted by
# an hour. Converting per-timestamp in SQL is correct in every window.
REQUEST_TIMEZONE = "UTC"

# Units pinned explicitly rather than relying on defaults: the risk thresholds in
# int_task_weather_risk are written in celsius, mm and km/h, so a change to the
# API's defaults would silently rescale every score.
UNITS = {
    "temperature_unit": "celsius",
    "precipitation_unit": "mm",
    "wind_speed_unit": "kmh",
}

MAX_ABS_LATITUDE = 90.0
MAX_ABS_LONGITUDE = 180.0

# Retried statuses: 429 is Open-Meteo's rate limit, the 5xx family is transient,
# 408 and 425 are request-level timing failures. Anything else (notably 400 for a
# malformed coordinate) is a bug in our request and retrying it just wastes time.
RETRY_STATUS_FORCELIST = (408, 425, 429, 500, 502, 503, 504)

DEFAULT_TIMEOUT = (10.0, 30.0)  # (connect, read) - 5s connect was too tight from a container


class ForecastUnavailable(RuntimeError):
    """A location's forecast could not be retrieved after retries.

    Raised per location so the caller can carry on with the others rather than
    losing a whole run to one bad response.
    """


@dataclass(frozen=True)
class Location:
    """A location to fetch a forecast for."""

    location_id: str
    latitude: float | None
    longitude: float | None
    timezone_name: str | None

    @property
    def has_valid_coordinates(self) -> bool:
        """Whether the coordinates are physically possible.

        The location seed has Perth's latitude and longitude transposed
        (lat=115.86, lng=-31.95), so this is not hypothetical. Checking here
        keeps a request that can only fail from reaching the API, and the
        affected tasks end up labelled 'unknown' rather than silently 'low'.
        """
        if self.latitude is None or self.longitude is None:
            return False
        return (
            abs(self.latitude) <= MAX_ABS_LATITUDE
            and abs(self.longitude) <= MAX_ABS_LONGITUDE
        )


def build_session(
    total_retries: int = 4,
    backoff_factor: float = 1.5,
) -> requests.Session:
    """A session that retries transient failures with exponential backoff.

    `respect_retry_after_header` matters for Open-Meteo specifically: it returns
    Retry-After on 429, and honouring it is both faster and politer than our own
    backoff schedule. `raise_on_status=False` lets the caller see the final
    response and log the body instead of getting a bare urllib3 error.
    """
    retry = Retry(
        total=total_retries,
        connect=total_retries,
        read=total_retries,
        status=total_retries,
        status_forcelist=RETRY_STATUS_FORCELIST,
        allowed_methods=frozenset(["GET"]),
        backoff_factor=backoff_factor,
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_forecast(
    session: requests.Session,
    location: Location,
    timeout: tuple[float, float] = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """Fetch one location's hourly forecast.

    Raises ForecastUnavailable on anything that leaves us without usable data.
    """
    if not location.has_valid_coordinates:
        raise ForecastUnavailable(
            f"{location.location_id}: refusing to request implausible coordinates "
            f"(lat={location.latitude}, lng={location.longitude})"
        )

    params: dict[str, Any] = {
        "latitude": location.latitude,
        "longitude": location.longitude,
        "hourly": ",".join(HOURLY_VARIABLES),
        "forecast_days": FORECAST_DAYS,
        "timezone": REQUEST_TIMEZONE,
        **UNITS,
    }

    try:
        response = session.get(FORECAST_URL, params=params, timeout=timeout)
    except requests.RequestException as exc:
        raise ForecastUnavailable(
            f"{location.location_id}: request failed after retries: {exc}"
        ) from exc

    if response.status_code != 200:
        # Open-Meteo puts a human-readable "reason" in the body on 400.
        raise ForecastUnavailable(
            f"{location.location_id}: HTTP {response.status_code}: "
            f"{response.text[:300]}"
        )

    try:
        payload = response.json()
    except ValueError as exc:
        raise ForecastUnavailable(
            f"{location.location_id}: response was not JSON: {exc}"
        ) from exc

    hourly = payload.get("hourly")
    if not isinstance(hourly, dict) or not hourly.get("time"):
        raise ForecastUnavailable(
            f"{location.location_id}: response contained no hourly series"
        )

    return payload


def _series(hourly: dict[str, Any], name: str, length: int) -> Sequence[Any]:
    """One variable's series, padded to `length` so a short array cannot misalign rows.

    A truncated series would otherwise shift every subsequent hour's values onto
    the wrong timestamp, which is far worse than a few nulls.
    """
    values = hourly.get(name) or []
    if len(values) >= length:
        return values[:length]
    return list(values) + [None] * (length - len(values))


def forecast_rows(
    location: Location,
    payload: dict[str, Any],
    fetched_at_utc: datetime,
    dagster_run_id: str | None = None,
) -> list[tuple[Any, ...]]:
    """Flatten Open-Meteo's columnar response into one row per forecast hour.

    The API returns parallel arrays (hourly.time[], hourly.temperature_2m[], ...).
    Column order here must match the DDL in assets/weather.py.
    """
    hourly = payload["hourly"]
    times = hourly["time"]
    n = len(times)

    temperature = _series(hourly, "temperature_2m", n)
    precipitation = _series(hourly, "precipitation", n)
    precipitation_probability = _series(hourly, "precipitation_probability", n)
    wind_speed = _series(hourly, "wind_speed_10m", n)
    wind_gusts = _series(hourly, "wind_gusts_10m", n)
    weather_code = _series(hourly, "weather_code", n)

    rows: list[tuple[Any, ...]] = []
    for i, raw_time in enumerate(times):
        # Requested with timezone=UTC, so these are naive UTC wall-clock stamps
        # ("2026-11-17T09:00"). Stored as TIMESTAMP and read as UTC downstream.
        forecast_time_utc = datetime.fromisoformat(raw_time)
        rows.append(
            (
                location.location_id,
                # Echoed back by the API: the grid cell it actually served, which
                # is not exactly the coordinate we asked for. Worth keeping to
                # explain why two nearby cities can share a forecast.
                payload.get("latitude"),
                payload.get("longitude"),
                REQUEST_TIMEZONE,
                forecast_time_utc,
                temperature[i],
                precipitation[i],
                precipitation_probability[i],
                wind_speed[i],
                wind_gusts[i],
                weather_code[i],
                fetched_at_utc,
                dagster_run_id,
            )
        )
    return rows
