{#
    One row per task_id. Parse, type, clean, deduplicate - no business rules.
    Eligibility ("which tasks have upcoming scheduled work") belongs in
    int_tasks_upcoming, not here.
#}

with source as (

    select * from {{ source('raw', 'raw_tasks') }}

),

parsed as (

    select
        trim(task_id)                                   as task_id,
        nullif(trim(title), '')                         as title,

        -- Lowercased for matching: ~5% of rows arrive lowercase, ~3% uppercase
        -- and ~4% with a trailing space. The canonical display name and the
        -- exposure weight both come from the category_weather_exposure seed,
        -- so an unmatched key is caught by a relationships test rather than
        -- silently costing the task its warning.
        lower(trim(category))                           as category_key,

        nullif(trim(location_id), '')                   as location_id,

        -- ~10% of states arrive capitalised ("Assigned"). The downstream
        -- eligibility filter is state = 'assigned', so without this a tenth of
        -- the eligible tasks would simply vanish.
        lower(trim(state))                              as state,

        -- Budgets arrive as '$284.50', '284.50' or '285'.
        try_cast(
            replace(replace(nullif(trim(budget), ''), '$', ''), ',', '')
            as decimal(10, 2)
        )                                               as budget,

        {{ parse_utc_timestamp('posted_at') }}          as posted_at_utc,
        {{ parse_utc_timestamp('assigned_at') }}        as assigned_at_utc,
        {{ parse_utc_timestamp('scheduled_at') }}       as scheduled_at_utc

    from source

),

deduplicated as (

    {#
        T003, T017 and T041 each appear twice with different posted_at values,
        from a replication glitch. Keep the most recent.

        This must run on the PARSED timestamp, not the raw text. Each duplicate
        pair is deliberately written in two different shapes - one
        '...T08:00:00Z' and one '2026-10-21 21:23:00+11:00' - and the second is
        the later instant but sorts EARLIER as a string, because ' ' (0x20)
        precedes 'T' (0x54). Ordering on the raw column picks the wrong record
        in all three pairs.

        The trailing hash makes the choice deterministic when two rows share a
        posted_at: without it the winner depends on scan order, and the model
        would not be reproducible.
    #}
    select *
    from parsed
    qualify row_number() over (
        partition by task_id
        order by
            posted_at_utc desc nulls last,
            assigned_at_utc desc nulls last,
            scheduled_at_utc desc nulls last,
            md5(
                concat_ws(
                    '|',
                    coalesce(title, ''),
                    coalesce(category_key, ''),
                    coalesce(location_id, ''),
                    coalesce(state, ''),
                    coalesce(cast(budget as varchar), '')
                )
            )
    ) = 1

)

select
    task_id,
    title,
    category_key,
    location_id,
    state,
    budget,
    posted_at_utc,
    assigned_at_utc,
    scheduled_at_utc
from deduplicated
