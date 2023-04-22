{{ config(materialized='view') }}

select
    loc.state,
    case
        when loc.state = 'WA' then 'Western Australia'
        when loc.state = 'QLD' then 'Queensland'
        when loc.state = 'NSW' then 'New South Wales'
        when loc.state = 'VIC' then 'Victoria'
        when loc.state = 'SA' then 'South Australia'
        when loc.state = 'TAS' then 'Tasmania'
        when loc.state = 'ACT' then 'Australian Capital Territory'
        when loc.state = 'NT' then 'Northern Territory'
        when loc.state = 'NZ' then 'New Zealand'
        when loc.state is not null then loc.state
    end as state_long,
    loc.country,
    case
        when loc.country = 'NZ' then 'New Zealand'
        when loc.country = 'AU' then 'Australia'
        else null
    end as country_long
from {{ ref('stg_locations') }} loc
where loc.state is not null
group by
    loc.state,
    loc.country
