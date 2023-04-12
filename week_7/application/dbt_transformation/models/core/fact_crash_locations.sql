{{ config(materialized='view') }}

select
    crashes.id,

    locations.id as location_id,
    locations.lat_long,
    locations.latitude,
    locations.longitude,
    locations.suburb,
    locations.country,
    locations.state,
    locations.local_government_area,

from {{ ref('fact_crashes') }} crashes
left join {{  ref('dim_locations') }} locations
on crashes.location_id = locations.id
