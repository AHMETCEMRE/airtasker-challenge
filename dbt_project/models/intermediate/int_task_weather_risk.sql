{#
    One row per upcoming scheduled task, with its weather risk score and label.

    THIS IS THE ONLY MODEL THAT SCORES. Both marts select from it:
    fct_task_weather_risk passes rows through for Product, and
    fct_location_daily_weather counts those same rows for Operations. That is
    what makes "both teams must report the same number of high-risk tasks for
    the same location and day" true by construction rather than by discipline.

    The model takes no clock and no configuration beyond the exposure weight
    it is handed, so a unit test can feed it fixtures and assert exact scores.

    ----------------------------------------------------------------------
    A NOTE ON NULLS, WHICH IS THE WHOLE DIFFICULTY HERE
    ----------------------------------------------------------------------
    DuckDB's greatest() and least() IGNORE nulls rather than propagating them.
    Verified in this warehouse:

        greatest(NULL, 5, 2) = 5        least(100, NULL) = 100
        greatest(0, NULL)    = 0        greatest(NULL, NULL) = NULL

    So the obvious formulation is actively dangerous:

        least(100, precip_mm_sum * 25)              -- no rain data -> 100
        least(100, greatest(0, (gusts - 20) / 0.4)) -- no wind data -> 0

    Missing precipitation data would score as a certainty of heavy rain, and
    missing wind data as dead calm. Every sub-score below therefore guards its
    inputs explicitly before letting greatest()/least() near them, and is NULL
    when it has nothing to say. Only once all three are NULL does the hazard
    become NULL, which is what produces the 'unknown' label.
#}

with tasks as (

    select * from {{ ref('int_tasks_upcoming') }}

),

weather as (

    select * from {{ ref('int_weather_window') }}

),

joined as (

    select
        t.*,

        w.window_start_utc,
        w.precip_probability_max,
        w.precip_mm_sum,
        w.wind_gusts_kmh_max,
        w.wind_speed_kmh_max,
        w.temperature_min_c,
        w.temperature_max_c,
        w.weather_code_modal,
        w.forecast_hours_covered,
        w.forecast_fetched_at_utc

    {#
        LEFT JOIN, not INNER. A task with no matching forecast window must
        still reach Product's mart labelled 'unknown'. There are three ways to
        get here with no weather:
          - an orphaned location_id (T058 -> LOC_MISSING_1)
          - a location whose coordinates were refused (Perth)
          - a task scheduled past the usable end of the forecast horizon
        An INNER JOIN would silently delete all three, which is the failure
        mode where the pipeline looks healthy and the warnings just never fire.
    #}
    from tasks as t
    left join weather as w
        on  t.location_id       = w.location_id
        and t.scheduled_hour_utc = w.window_start_utc

),

scored as (

    select
        j.*,

        {#
            PRECIPITATION, 0-100.
            Probability and intensity are combined with greatest() because
            either alone is enough: a 90% chance of light rain and a certainty
            of 4mm are both bad news for a roof clean. 4mm over the window
            saturates the intensity term.
        #}
        case
            when j.precip_probability_max is null and j.precip_mm_sum is null
                then null
            else greatest(
                coalesce(j.precip_probability_max, 0.0),
                case
                    when j.precip_mm_sum is null then 0.0
                    else least(100.0, j.precip_mm_sum * 25.0)
                end
            )
        end                                                 as precip_score,

        {#
            WIND, 0-100, from gusts rather than mean speed: a ladder or a
            marquee fails on the gust, not the average. 20 km/h is a non-event,
            60 km/h and above stops outdoor work.
        #}
        case
            when j.wind_gusts_kmh_max is null
                then null
            else least(100.0, greatest(0.0, (j.wind_gusts_kmh_max - 20.0) / 0.40))
        end                                                 as wind_score,

        {#
            TEMPERATURE, 0-100. Two independent hazards, so the band is scored
            from both ends and the worse one wins:
              heat: 32C is fine, 42C is unworkable
              cold:  5C is fine, -5C is unworkable
            Relevant in both directions here - Darwin in November and
            Edinburgh in December are different problems.
        #}
        case
            when j.temperature_min_c is null and j.temperature_max_c is null
                then null
            else greatest(
                case
                    when j.temperature_max_c is null       then 0.0
                    when j.temperature_max_c <= 32.0       then 0.0
                    else least(100.0, (j.temperature_max_c - 32.0) / 10.0 * 100.0)
                end,
                case
                    when j.temperature_min_c is null       then 0.0
                    when j.temperature_min_c >= 5.0        then 0.0
                    else least(100.0, (5.0 - j.temperature_min_c) / 10.0 * 100.0)
                end
            )
        end                                                 as temp_score

    from joined as j

),

hazard as (

    select
        s.*,

        {#
            The worst single hazard, not a weighted sum.

            Risk is a disjunction: one severe hazard is enough to stop the job,
            and averaging would dilute a 100% rain probability into a
            "medium". The cost, stated plainly, is that this ignores
            compounding - 8C, wet and gusty is worse than any one of those
            alone and scores here as only the worst. Fixing that properly means
            fitting coefficients against real reschedule and cancellation
            rates, which is why the risk history snapshot exists.

            greatest() ignoring nulls is wanted here: partial weather data
            still yields the worst of what is known. It returns NULL only when
            all three sub-scores are NULL, i.e. we know nothing at all.
        #}
        greatest(s.precip_score, s.wind_score, s.temp_score) as hazard_score

    from scored as s

),

final as (

    select
        h.*,

        {#
            Weather only matters to the extent the work is exposed to it.

            Order of the branches is deliberate:

            1. exposure_weight IS NULL -> NULL. The category did not match the
               seed. A relationships test fails the build on this, so it should
               be unreachable; scoring it 0 would quietly drop a real warning.

            2. exposure_weight = 0 -> 0, even with no forecast at all. An
               online tutoring session is not weather-sensitive, and that is a
               fact about the category, not missing data. These must never be
               'unknown' or they would pollute Operations' unknown counts with
               tasks that were never at risk.

            3. hazard_score IS NULL -> NULL -> 'unknown'. Explicitly NOT 'low'.
               Missing data is not safe data.
        #}
        case
            when h.exposure_weight is null  then null
            when h.exposure_weight = 0      then 0
            when h.hazard_score is null     then null
            else cast(round(h.hazard_score * h.exposure_weight) as integer)
        end                                                 as weather_risk_score

    from hazard as h

)

select
    task_id,
    title,
    category,
    category_key,
    exposure_class,
    exposure_weight,

    location_id,
    city,
    country,
    timezone_name,
    is_valid_coordinates,

    budget,
    posted_at_utc,
    assigned_at_utc,
    scheduled_at_utc,
    scheduled_hour_utc,
    scheduled_at_local,
    scheduled_local_date,

    -- Weather the score was computed from, carried through so a warning can be
    -- explained to a poster without re-querying the forecast.
    precip_probability_max,
    precip_mm_sum,
    wind_gusts_kmh_max,
    wind_speed_kmh_max,
    temperature_min_c,
    temperature_max_c,
    weather_code_modal,
    forecast_hours_covered,
    forecast_fetched_at_utc,

    -- Sub-scores kept for explainability: "why is this high" should be
    -- answerable from the row itself.
    precip_score,
    wind_score,
    temp_score,
    hazard_score,

    weather_risk_score,
    {{ weather_risk_label('weather_risk_score') }}          as weather_risk_label,

    (window_start_utc is not null)                          as has_forecast

from final
