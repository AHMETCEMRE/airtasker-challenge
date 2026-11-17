{#
    Product's mart: one row per task with upcoming scheduled work.

    One row per TASK, not per task per day - a task has a single scheduled
    time, so its risk is a scalar. This is the table the CRM reads to decide
    who gets warned.

    Deliberately a thin pass-through over int_task_weather_risk rather than a
    model that recomputes anything. The moment this file contains a CASE
    expression over thresholds, it can disagree with Operations' mart, which is
    the one thing the brief says must never happen.
#}

select
    -- Grain
    task_id,

    -- What the poster sees in the warning
    title,
    category,
    city,
    country,
    scheduled_at_local,
    scheduled_local_date,

    -- The verdict
    weather_risk_score,
    weather_risk_label,

    -- Why: enough to explain the verdict without re-querying the forecast
    precip_probability_max,
    precip_mm_sum,
    wind_gusts_kmh_max,
    temperature_min_c,
    temperature_max_c,
    weather_code_modal,
    precip_score,
    wind_score,
    temp_score,
    hazard_score,
    exposure_class,
    exposure_weight,

    -- Provenance and caveats
    has_forecast,
    forecast_hours_covered,
    forecast_fetched_at_utc,
    is_valid_coordinates,

    -- Keys and timings for joining back to the rest of the warehouse
    location_id,
    timezone_name,
    budget,
    posted_at_utc,
    assigned_at_utc,
    scheduled_at_utc,
    scheduled_hour_utc

from {{ ref('int_task_weather_risk') }}
