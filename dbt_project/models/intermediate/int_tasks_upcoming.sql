{#
    One row per task with upcoming scheduled work: who to warn, when, and where.

    This model owns two things and deliberately not a third.

    It owns ELIGIBILITY - which tasks the CRM should consider at all - and
    LOCALISATION - putting each task on a local calendar day and hour.

    It does not own SCORING. Keeping the clock here and the arithmetic in
    int_task_weather_risk is what makes the risk logic unit-testable: a unit
    test can feed this model's output as a fixture and get a deterministic
    score, with no dependence on when the test happens to run.
#}

with upcoming as (

    select
        t.task_id,
        t.title,
        t.category_key,
        t.location_id,
        t.state,
        t.budget,
        t.posted_at_utc,
        t.assigned_at_utc,

        -- Truncated to the hour so it joins cleanly onto the hourly forecast
        -- grid. The source only ever writes whole hours, but a task scheduled
        -- at 09:30 should be scored against the 09:00 window rather than
        -- silently failing to match and coming back 'unknown'.
        date_trunc('hour', t.scheduled_at_utc)  as scheduled_hour_utc,
        t.scheduled_at_utc

    from {{ ref('stg_tasks') }} as t

    where
        {#
            'assigned' only.

            - completed: the work already happened, there is nothing to warn about.
            - open: no scheduled_at exists, so there is no weather to look up.
            - cancelled: excluded even though roughly half of cancelled tasks
              carry a future scheduled_at. This feeds a CRM warning, and telling
              a poster their cancelled job is at risk is worse than saying
              nothing. Costs 7 rows on the current data - a real number, not a
              hypothetical.
        #}
        t.state = 'assigned'
        and t.scheduled_at_utc is not null
        and t.scheduled_at_utc >= {{ risk_as_of_utc() }}

),

localised as (

    select
        u.*,

        l.city,
        l.country,
        l.timezone_name,
        l.is_valid_coordinates,

        {#
            UTC day is not local day. scheduled_at lands at UTC hours 08-15,
            which in Australia (+10/+11) is 18:00-01:00 local, so a chunk of AU
            tasks belong to the NEXT local day: 11 of 105 tasks in the current
            data have a local date that differs from their UTC date.

            Operations asked for days cut at midnight in the location's own
            timezone, so this conversion is the whole basis of their mart.
            Converting via the IANA name rather than a numeric offset is what
            makes Adelaide and Darwin (+09:30) and Phoenix (no DST) correct.

            location_id is a LEFT JOIN: two tasks reference locations that do
            not exist, and dropping them here would mean nobody ever finds out.
        #}
        case
            when l.timezone_name is not null
            then (u.scheduled_hour_utc at time zone 'UTC' at time zone l.timezone_name)
        end                                     as scheduled_at_local,

        case
            when l.timezone_name is not null
            then (u.scheduled_hour_utc at time zone 'UTC' at time zone l.timezone_name)::date
        end                                     as scheduled_local_date

    from upcoming as u
    left join {{ ref('stg_locations') }} as l
        on u.location_id = l.location_id

)

select
    lo.task_id,
    lo.title,

    -- Canonical display name and exposure weight come from the seed, so the
    -- mart shows "Roof Cleaning" rather than the lowercased join key, and the
    -- scoring model receives the weight as plain input.
    coalesce(e.category, lo.category_key)       as category,
    lo.category_key,
    e.exposure_class,
    e.exposure_weight,

    lo.location_id,
    lo.city,
    lo.country,
    lo.timezone_name,
    lo.is_valid_coordinates,

    lo.state,
    lo.budget,
    lo.posted_at_utc,
    lo.assigned_at_utc,
    lo.scheduled_at_utc,
    lo.scheduled_hour_utc,
    lo.scheduled_at_local,
    lo.scheduled_local_date

from localised as lo
left join {{ ref('category_weather_exposure') }} as e
    on lo.category_key = e.category_key
