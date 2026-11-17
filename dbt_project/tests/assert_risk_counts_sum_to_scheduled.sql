{#
    The four risk-label counts must account for every scheduled task on a
    location-day.

    This is what stops 'unknown' from being quietly swept under the rug. If
    someone later decides unknown tasks are "basically low" and drops the
    column, or filters them out of the count, this test fails: the remaining
    counts will no longer sum to scheduled_task_count and Operations' table
    becomes internally inconsistent.

    It also catches a label appearing that nobody accounted for, since a fifth
    label would be counted in scheduled_task_count but in none of the four.
#}

select
    location_id,
    local_date,
    scheduled_task_count,
    high_risk_task_count,
    medium_risk_task_count,
    low_risk_task_count,
    unknown_risk_task_count,
    (
        high_risk_task_count
        + medium_risk_task_count
        + low_risk_task_count
        + unknown_risk_task_count
    )                                               as summed_label_counts

from {{ ref('fct_location_daily_weather') }}

where scheduled_task_count <> (
        high_risk_task_count
        + medium_risk_task_count
        + low_risk_task_count
        + unknown_risk_task_count
    )
