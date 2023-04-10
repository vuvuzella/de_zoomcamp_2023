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

from {{ ref('crashes') }} crashes
left join {{  ref('locations') }} locations
on crashes.location_id = locations.id
