{#
    Operations' mart: one row per location per local day.

    Every count here is a count of rows produced by int_task_weather_risk -
    the same model fct_task_weather_risk passes through for Product. Nothing is
    rescored and no threshold is re-expressed, so the two marts cannot disagree
    about how many tasks are high risk on a given location-day. That property
    is asserted by tests/assert_marts_agree_on_daily_counts.sql rather than
    being left as a claim.

    Spans the whole forecast horizon, not only days that have tasks, so
    Operations gets a continuous calendar with scheduled_task_count = 0 on
    quiet days instead of gaps they have to interpret.
#}

with days as (

    select * from {{ ref('int_location_days') }}

),

task_counts as (

    select
        location_id,
        scheduled_local_date                        as local_date,

        count(*)                                    as scheduled_task_count,

        -- The number Product and Operations must agree on.
        count_if(weather_risk_label = 'high')       as high_risk_task_count,
        count_if(weather_risk_label = 'medium')     as medium_risk_task_count,
        count_if(weather_risk_label = 'low')        as low_risk_task_count,

        {#
            Reported, not hidden. These are tasks the pipeline cannot speak
            about - refused coordinates, an orphaned location, or a day past
            the usable horizon. Folding them into 'low' would present a data
            gap as a safe day; omitting the column would make the four risk
            counts fail to sum to scheduled_task_count, and Operations would
            rightly stop trusting the table.
        #}
        count_if(weather_risk_label = 'unknown')    as unknown_risk_task_count,

        max(weather_risk_score)                     as max_weather_risk_score,
        round(avg(weather_risk_score), 1)           as avg_weather_risk_score

    from {{ ref('int_task_weather_risk') }}

    {#
        Tasks with no local date cannot be placed on anyone's calendar. That is
        the orphaned-location case (T058 -> LOC_MISSING_1): it has no timezone,
        so no local day. It stays visible in Product's mart as 'unknown' and is
        absent here, which is why the reconciliation property is scoped to
        locations that exist in the seed.
    #}
    where scheduled_local_date is not null

    group by
        location_id,
        scheduled_local_date

)

select
    -- Grain
    d.location_id,
    d.local_date,

    -- Who and where
    d.city,
    d.country,
    d.timezone_name,

    -- What Operations staffs against
    coalesce(t.scheduled_task_count, 0)         as scheduled_task_count,
    coalesce(t.high_risk_task_count, 0)         as high_risk_task_count,
    coalesce(t.medium_risk_task_count, 0)       as medium_risk_task_count,
    coalesce(t.low_risk_task_count, 0)          as low_risk_task_count,
    coalesce(t.unknown_risk_task_count, 0)      as unknown_risk_task_count,
    t.max_weather_risk_score,
    t.avg_weather_risk_score,

    -- That day's weather, cut at local midnight.
    -- Both a typical and a worst reading: the modal code alone calls a 5.7mm
    -- Seattle day "Clear sky", because clear hours hold the plurality.
    d.weather_description,
    d.weather_description_worst,
    d.weather_code_modal,
    d.weather_code_worst,
    d.precip_probability_max,
    d.precip_mm_total,
    d.wind_gusts_kmh_max,
    d.wind_speed_kmh_max,
    d.temperature_min_c,
    d.temperature_max_c,

    -- Caveats, so a half-observed day is not read as a mild one
    d.forecast_hours,
    d.is_complete_local_day,
    d.is_valid_coordinates,
    d.forecast_fetched_at_utc

from days as d
left join task_counts as t
    on  d.location_id = t.location_id
    and d.local_date  = t.local_date
