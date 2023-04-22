{{ config(materialized='view') }}

select
    c.id,
    l.lat_long,
    l.latitude,
    l.longitude,
    l.country,
    l.state,
    l.suburb,
    case
        when l.country = 'NZ' then 'New Zealand'
        when l.country = 'AU' then 'Australia'
        else null
    end as country_long,
    case
        when l.state = 'WA' then 'Western Australia'
        when l.state = 'QLD' then 'Queensland'
        when l.state = 'NSW' then 'New South Wales'
        when l.state = 'VIC' then 'Victoria'
        when l.state = 'SA' then 'South Australia'
        when l.state = 'TAS' then 'Tasmania'
        when l.state = 'ACT' then 'Australian Capital Territory'
        when l.state = 'NT' then 'Northern Territory'
        when l.state = 'NZ' then 'New Zealand'
        when l.state is not null then l.state
        else null
    end as state_long,
from {{ ref('stg_crashes') }} c
left join {{ ref('stg_locations') }} l
on c.location_id = l.id
