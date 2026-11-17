{#
    "Both teams must always report the same number of high-risk tasks for the
    same location and day."

    That is the one hard requirement in the brief, and the architecture is
    built to make it true by construction: both marts descend from
    int_task_weather_risk, and the label comes from a single macro. This test
    exists because "true by construction" is a claim about code that someone
    will eventually refactor. It fails the build the moment the two marts
    diverge, for any reason.

    Scoped to locations that exist in the seed. A task with an orphaned
    location_id (T058 -> LOC_MISSING_1) has no timezone, therefore no local
    day, so it appears in Product's mart as 'unknown' and cannot appear in
    Operations' calendar at all. That asymmetry is intentional and documented;
    this test asserts agreement everywhere it is meaningful.

    A FULL OUTER JOIN rather than an inner one, so the test also catches a
    location-day present on one side and missing from the other - which is the
    failure a naive inner-join check would sail straight past.
#}

with product_side as (

    select
        location_id,
        scheduled_local_date                        as local_date,
        count(*)                                    as scheduled_task_count,
        count_if(weather_risk_label = 'high')       as high_risk_task_count
    from {{ ref('fct_task_weather_risk') }}
    where scheduled_local_date is not null
      and location_id in (select location_id from {{ ref('stg_locations') }})
    group by location_id, scheduled_local_date

),

operations_side as (

    select
        location_id,
        local_date,
        scheduled_task_count,
        high_risk_task_count
    from {{ ref('fct_location_daily_weather') }}
    -- Quiet days are legitimately zero on the Operations side and simply
    -- absent on the Product side; only days with tasks are comparable.
    where scheduled_task_count > 0

)

select
    coalesce(p.location_id, o.location_id)          as location_id,
    coalesce(p.local_date, o.local_date)            as local_date,
    p.scheduled_task_count                          as product_scheduled_count,
    o.scheduled_task_count                          as operations_scheduled_count,
    p.high_risk_task_count                          as product_high_risk_count,
    o.high_risk_task_count                          as operations_high_risk_count,
    case
        when p.location_id is null then 'missing from Product mart'
        when o.location_id is null then 'missing from Operations mart'
        when p.scheduled_task_count <> o.scheduled_task_count
            then 'scheduled_task_count disagrees'
        else 'high_risk_task_count disagrees'
    end                                             as failure_reason

from product_side as p
full outer join operations_side as o
    on  p.location_id = o.location_id
    and p.local_date  = o.local_date

where p.location_id is null
   or o.location_id is null
   or p.scheduled_task_count  <> o.scheduled_task_count
   or p.high_risk_task_count  <> o.high_risk_task_count
