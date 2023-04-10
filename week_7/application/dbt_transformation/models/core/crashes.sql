
{{ config(materialized='view') }}

select

    {{ dbt_utils.generate_surrogate_key(['crash_id']) }} as id,
    {{ dbt_utils.generate_surrogate_key(['date_time_id']) }} as datetime_id,
    {{ dbt_utils.generate_surrogate_key(['description_id']) }} as description_id,
    {{ dbt_utils.generate_surrogate_key(['vehicles_id']) }} as vehicles_id,
    {{ dbt_utils.generate_surrogate_key(['casualties_id'])}} as casualties_id,
    {{ dbt_utils.generate_surrogate_key(['lat_long']) }} as location_id,

    {{ get_latitude('lat_long') }} as latitude,
    {{ get_longitude('lat_long') }} as longitude,

from {{ source('anz_road_crash_dataset', 'raw_crashes') }}
