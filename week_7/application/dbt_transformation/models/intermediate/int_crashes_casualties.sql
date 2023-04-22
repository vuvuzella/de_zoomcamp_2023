{{ config(materialized='view') }}

select
    crashes.id as id,
    casualties.casualties,
    casualties.fatalities,
    casualties.serious_injuries,
    casualties.minor_injuries
from {{ ref('stg_crashes') }} crashes
left join {{ ref('stg_casualties') }} casualties
on crashes.casualties_id = casualties.id
