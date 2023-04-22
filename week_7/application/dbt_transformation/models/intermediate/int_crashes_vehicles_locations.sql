{{ config(materialized='view') }}


select
    v.*,
    l.country,
    l.state,
    l.suburb,
    l.state_long
from {{ ref('int_crashes_vehicles') }} v
left join {{ ref('int_crashes_locations') }} l
on v.id = l.id
