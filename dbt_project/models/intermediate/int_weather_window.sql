{#
    One row per (location_id, window_start_utc): the weather a job starting at
    that hour would actually experience.

    A task is scored over a working window rather than a single hour, because
    marketplace jobs take hours - a roof clean starting at 9am is exposed to
    10am and 11am too. The window length is var('risk_window_hours').

    Built for EVERY forecast hour, not just hours that happen to have a task
    scheduled. Same reasoning as fetching forecasts for every location: the
    model stays independent of task-side business rules, so it does not change
    meaning when those rules change, and Operations' daily rollup can aggregate
    from it without worrying about which hours tasks occupy. At 7,296 forecast
    hours the self-join is trivial.

    Aggregates are chosen per variable rather than uniformly:
      - precipitation_probability: MAX, because any hour of likely rain matters
      - precipitation: SUM, because total fall is what makes ground unworkable
      - wind_gusts: MAX, because a single gust is what takes a ladder down
      - temperature: MIN and MAX, since heat and cold are separate hazards
#}

with forecast as (

    select * from {{ ref('stg_weather_forecast') }}

),

windowed as (

    select
        start_hour.location_id,
        start_hour.forecast_time_utc                    as window_start_utc,

        max(in_window.precipitation_probability)        as precip_probability_max,
        sum(in_window.precipitation_mm)                 as precip_mm_sum,
        max(in_window.wind_gusts_kmh)                   as wind_gusts_kmh_max,
        max(in_window.wind_speed_kmh)                   as wind_speed_kmh_max,
        min(in_window.temperature_2m_c)                 as temperature_min_c,
        max(in_window.temperature_2m_c)                 as temperature_max_c,

        -- Most frequent WMO code across the window, for Operations' summary.
        mode(in_window.weather_code)                    as weather_code_modal,

        -- How many forecast hours the window actually covers. Less than
        -- risk_window_hours means the window runs off the end of the horizon,
        -- where Open-Meteo's model data thins out. Carried through so the
        -- scoring model can tell "calm" from "we do not know".
        count(in_window.forecast_time_utc)              as forecast_hours_covered,

        -- Which hours of weather actually carried values. A window can have
        -- three rows but null measurements in the last day or two of the
        -- 16-day horizon.
        count(in_window.precipitation_probability)      as precip_probability_hours,
        count(in_window.wind_gusts_kmh)                 as wind_gusts_hours,
        count(in_window.temperature_2m_c)               as temperature_hours,

        max(in_window.fetched_at_utc)                   as forecast_fetched_at_utc

    from forecast as start_hour
    inner join forecast as in_window
        on  in_window.location_id       = start_hour.location_id
        and in_window.forecast_time_utc >= start_hour.forecast_time_utc
        and in_window.forecast_time_utc <  start_hour.forecast_time_utc
                                           + interval {{ var('risk_window_hours') }} hour

    group by
        start_hour.location_id,
        start_hour.forecast_time_utc

)

select
    location_id,
    window_start_utc,
    precip_probability_max,
    precip_mm_sum,
    wind_gusts_kmh_max,
    wind_speed_kmh_max,
    temperature_min_c,
    temperature_max_c,
    weather_code_modal,
    forecast_hours_covered,
    precip_probability_hours,
    wind_gusts_hours,
    temperature_hours,
    forecast_fetched_at_utc
from windowed
