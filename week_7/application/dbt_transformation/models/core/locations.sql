{{ config(materialized='view') }}

select
    {{ dbt_utils.generate_surrogate_key(['lat_long']) }} as id,
    cast(lat_long as string) as lat_long,
    cast(latitude as float64) as latitude,
    cast(longitude as float64) as longitude,
    cast(country as string) as country,
    cast(state as string) as state,
    case
        when local_government_area = 'nan' then null
        else cast(local_government_area as string)
    end as local_government_area,
    cast(suburb as string) as suburb

from {{ source('anz_road_crash_dataset', 'raw_locations')}}
