{#
    One row per location_id. The seed stores every column as text.
#}

with source as (

    select * from {{ ref('raw_locations') }}

),

cleaned as (

    select
        trim(location_id)                   as location_id,
        nullif(trim(city), '')              as city,

        -- Leeds and Glasgow arrive as 'UK' while the other British cities are
        -- 'GB'. ISO 3166-1 alpha-2 for the United Kingdom is GB, so normalise
        -- rather than carry two codes for one country.
        case
            when upper(trim(country)) = 'UK' then 'GB'
            else upper(trim(country))
        end                                 as country,

        try_cast(trim(lat) as double)       as latitude,
        try_cast(trim(lng) as double)       as longitude,
        nullif(trim(timezone), '')          as timezone_name

    from source

)

select
    location_id,
    city,
    country,
    latitude,
    longitude,
    timezone_name,

    {#
        Perth arrives as lat=115.8605, lng=-31.9505: the pair is transposed, and
        a latitude of 115 does not exist. Flagged rather than swapped.

        Auto-swapping would hide a source data bug and cannot distinguish a
        transposition from a coordinate that is simply wrong, so the pipeline
        refuses to guess: the ingestion asset will not send these coordinates to
        Open-Meteo, and tasks at this location end up scored 'unknown' instead
        of being given a forecast for the Indian Ocean.
    #}
    (
        latitude  is not null
        and longitude is not null
        and latitude  between  -90 and  90
        and longitude between -180 and 180
    )                                       as is_valid_coordinates

from cleaned
