
{{ config(materialized='view') }}

select

    cast(crash_id as string) as id,
    cast(date_time_id as string) as date_time_id,
    cast(description_id as int) as description_id,
    cast(vehicles_id as string) as vehicles_id,
    cast(casualties_id as string) as casualties_id,
    {{ dbt_utils.generate_surrogate_key(['lat_long']) }} as location_id,
    {{ get_latitude('lat_long') }} as latitude,
    {{ get_longitude('lat_long') }} as longitude,

from {{ source('anz_road_crash_dataset', 'raw_crashes') }}
