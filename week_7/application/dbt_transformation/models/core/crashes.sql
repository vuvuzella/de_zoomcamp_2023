
{{ config(materialized='view') }}

select

    cast(crash_id as string) as id,
    cast(date_time_id as string) as date_time_id,
    cast(description_id as int) as description_id,
    cast(vehicles_id as string) as vehicles_id,
    cast(casualties_id as string) as casualties_id,
    trim(split(replace(replace(lat_long, '(', ''), ')', '' ), ',')[offset(0)]) as latitude,
    trim(split(replace(replace(lat_long, '(', ''), ')', '' ), ',')[offset(1)]) as longitude,

from {{ source('anz_road_crash_dataset', 'raw_crashes') }}
