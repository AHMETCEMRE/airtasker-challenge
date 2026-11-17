{#
    Risk history: how a task's weather risk changed between pipeline runs.

    The brief asks for this directly - "Product also wants to see how a task's
    risk changed between pipeline runs" - and it is the one requirement that
    cannot be answered by a mart, because a mart only holds the current answer.

    WHY A SNAPSHOT RATHER THAN AN EVENT LOG IN THE MART.
    Making fct_task_weather_risk append-only would have been the cheap route,
    but it would change that table's grain from one row per task to one row per
    task per run. Every consumer would then have to remember to filter to the
    latest row, and the first person who forgets double-counts their warnings.
    A snapshot keeps the mart at one row per task and puts the history beside
    it, where asking for history is explicit.

    STRATEGY: check, on the score and the label only.
    Product's question is "did the risk change", not "did the forecast change".
    Forecasts shift every few hours; the verdict usually does not. Checking the
    two output columns means a row is written when the answer moves, rather than
    every run - the difference between a history someone can read and a table
    that doubles in size daily.

    The weather inputs are carried along for explanation ("it went high because
    the gust forecast doubled") but are not checked, so they are updated in
    place on the current row rather than generating a new one.

    hard_deletes='invalidate': when a task drops out of the mart - its
    scheduled time has passed, or it was cancelled - dbt closes the record
    instead of leaving it looking current forever. "We stopped warning about
    this" is itself a fact Product may want.
#}

{% snapshot snp_task_weather_risk %}

{{
    config(
        target_schema='snapshots',
        unique_key='task_id',
        strategy='check',
        check_cols=['weather_risk_score', 'weather_risk_label'],
        hard_deletes='invalidate',
    )
}}

select
    -- Identity
    task_id,

    -- The verdict, and the only thing a change is detected on
    weather_risk_score,
    weather_risk_label,

    -- Enough context to read the history without joining back to the mart,
    -- which may itself have moved on
    category,
    city,
    location_id,
    scheduled_at_utc,
    scheduled_local_date,

    -- Why the verdict was what it was at the time
    hazard_score,
    precip_score,
    wind_score,
    temp_score,
    precip_probability_max,
    precip_mm_sum,
    wind_gusts_kmh_max,
    temperature_min_c,
    temperature_max_c,
    has_forecast,

    -- Which fetch the verdict was based on, so a change can be attributed to a
    -- new forecast rather than a code change
    forecast_fetched_at_utc

from {{ ref('fct_task_weather_risk') }}

{% endsnapshot %}
