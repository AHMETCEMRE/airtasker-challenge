{#
    One row per (location_id, local_date): the calendar spine Operations' mart
    is built on, with each day's weather aggregated between LOCAL midnights.

    Two decisions here carry most of the weight.

    1. THE SPINE COMES FROM LOCATIONS, NOT FROM WEATHER.
       Perth's coordinates were refused by ingestion, so it has no forecast
       rows at all. A spine derived from stg_weather_forecast would contain no
       Perth days, and Perth's 7 scheduled tasks would silently vanish from
       Operations' view - which are precisely the tasks Operations most needs
       to see, because they are the ones nobody can forecast. Starting from
       stg_locations and left-joining the weather means Perth appears with null
       weather and a visible task count, which is actionable: fix the seed.

    2. DAYS ARE CUT AT LOCAL MIDNIGHT, PER LOCATION.
       Operations asked for exactly this. Converting each forecast hour through
       the location's IANA name before taking its date is what makes Adelaide
       and Darwin (+09:30) and Phoenix (no DST) correct, where a numeric offset
       would not be.

       A consequence worth knowing: the first and last local day of a 16-day
       UTC window are partial. Sydney's first local day holds 13 hours and its
       last holds 11, because the UTC window does not start or end at Sydney
       midnight. Those days are kept rather than dropped - today has tasks on
       it - but forecast_hours and is_complete_local_day are exposed so a
       consumer can tell a genuinely mild day from a half-observed one.
#}

with horizon as (

    select
        min(forecast_time_utc) as first_forecast_utc,
        max(forecast_time_utc) as last_forecast_utc
    from {{ ref('stg_weather_forecast') }}

),

location_bounds as (

    select
        l.location_id,
        l.city,
        l.country,
        l.timezone_name,
        l.is_valid_coordinates,

        -- The local calendar window differs per location: Los Angeles starts a
        -- day earlier than UTC, Sydney ends a day later.
        (h.first_forecast_utc at time zone 'UTC' at time zone l.timezone_name)::date
            as first_local_date,
        (h.last_forecast_utc  at time zone 'UTC' at time zone l.timezone_name)::date
            as last_local_date

    from {{ ref('stg_locations') }} as l
    cross join horizon as h

    -- A location with no timezone cannot be placed on a local calendar at all.
    -- The staging not_null test already fails the build on this.
    where l.timezone_name is not null

),

spine as (

    select
        lb.location_id,
        lb.city,
        lb.country,
        lb.timezone_name,
        lb.is_valid_coordinates,
        unnest(
            generate_series(lb.first_local_date, lb.last_local_date, interval 1 day)
        )::date as local_date
    from location_bounds as lb

),

daily_weather as (

    select
        f.location_id,
        (f.forecast_time_utc at time zone 'UTC' at time zone l.timezone_name)::date
            as local_date,

        count(*)                                as forecast_hours,

        max(f.precipitation_probability)        as precip_probability_max,
        sum(f.precipitation_mm)                 as precip_mm_total,
        max(f.wind_gusts_kmh)                   as wind_gusts_kmh_max,
        max(f.wind_speed_kmh)                   as wind_speed_kmh_max,
        min(f.temperature_2m_c)                 as temperature_min_c,
        max(f.temperature_2m_c)                 as temperature_max_c,
        {#
            Two codes, because one is not enough.

            MODE is the day's general character - what it mostly looks like.
            On its own it misleads: Seattle on 28 Nov has 5.7mm of rain but a
            plurality of clear hours, so the modal code alone describes it as
            "Clear sky", which no ops analyst would accept.

            MAX is the worst condition the day reaches. WMO 4677 is broadly
            ordered by increasing severity, so the maximum is a reasonable
            proxy for "the worst it gets" - imperfect across families (snow
            versus showers is arguable) but decision-relevant in a way the
            mode is not.
        #}
        mode(f.weather_code)                    as weather_code_modal,
        max(f.weather_code)                     as weather_code_worst,
        max(f.fetched_at_utc)                   as forecast_fetched_at_utc

    from {{ ref('stg_weather_forecast') }} as f
    inner join {{ ref('stg_locations') }} as l
        on f.location_id = l.location_id

    where l.timezone_name is not null

    group by
        f.location_id,
        (f.forecast_time_utc at time zone 'UTC' at time zone l.timezone_name)::date

)

select
    s.location_id,
    s.city,
    s.country,
    s.timezone_name,
    s.is_valid_coordinates,
    s.local_date,

    coalesce(w.forecast_hours, 0)               as forecast_hours,
    (coalesce(w.forecast_hours, 0) = 24)        as is_complete_local_day,

    w.precip_probability_max,
    w.precip_mm_total,
    w.wind_gusts_kmh_max,
    w.wind_speed_kmh_max,
    w.temperature_min_c,
    w.temperature_max_c,
    w.weather_code_modal,
    {{ wmo_weather_description('w.weather_code_modal') }} as weather_description,
    w.weather_code_worst,
    {{ wmo_weather_description('w.weather_code_worst') }} as weather_description_worst,
    w.forecast_fetched_at_utc

from spine as s
left join daily_weather as w
    on  s.location_id = w.location_id
    and s.local_date  = w.local_date
