{{ config(materialized='view') }}

select
    c.id,
    l.latitude,
    l.longitude,
    l.country,
    l.state,
    l.suburb
from {{ ref('stg_crashes') }} c
left join {{ ref('stg_locations') }} l
on c.location_id = l.id
