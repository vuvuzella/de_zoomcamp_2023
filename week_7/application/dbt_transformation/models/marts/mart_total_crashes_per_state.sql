{{ config(materialized='view') }}

select
    l.state,
    count(l.id) as total_crashes
from {{ ref('int_crashes_locations') }} l
where l.state is not null
group by
    l.state
