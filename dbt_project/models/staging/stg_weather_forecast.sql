{#
    One row per (location_id, forecast_time_utc): the current view of the
    forecast, taken from the most recent fetch that covers each hour.

    raw.raw_weather_forecast is append-only and keeps every fetch, which is what
    lets Product compare a task's risk between pipeline runs. This model is the
    "what do we believe right now" layer on top of that history.
#}

with source as (

    select * from {{ source('raw', 'raw_weather_forecast') }}

)

select
    location_id,
    forecast_time_utc,
    temperature_2m_c,
    precipitation_mm,
    precipitation_probability,
    wind_speed_kmh,
    wind_gusts_kmh,
    weather_code,
    fetched_at_utc,
    dagster_run_id
from source

{#
    Partitioned by (location, hour) rather than filtering to a single global
    max(fetched_at_utc). That distinction matters: a run where some locations
    fail - which happens, three locations timed out on the first real run -
    would otherwise drop every location the newest fetch happened to miss.
    Keyed this way, each location independently resolves to its own latest
    fetch, so a partial run tops up the gaps instead of truncating the table.
#}
qualify row_number() over (
    partition by location_id, forecast_time_utc
    order by fetched_at_utc desc
) = 1
