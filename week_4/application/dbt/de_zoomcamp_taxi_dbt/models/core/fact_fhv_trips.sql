{{ config(materialized='table') }}

with fhv_data as (
    select
        *
    from {{ ref('stage_fhv_trip_data') }}
),

dim_zones as (
    select
        *
    from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    fhv_data.dispatching_base_num
    ,fhv_data.pickup_datetime
    ,fhv_data.dropoff_datetime
    ,fhv_data.pickup_locationid
    ,pickup_zones.borough as pickup_borough
    ,pickup_zones.zone as pickup_zone
    ,fhv_data.dropoff_locationid
    ,dropoff_zones.borough as dropoff_borough
    ,dropoff_zones.zone as dropoff_zone
    ,fhv_data.sr_flag
    ,fhv_data.affiliated_base_number
from fhv_data
inner join dim_zones as pickup_zones
on fhv_data.pickup_locationid = pickup_zones.locationid
inner join dim_zones as dropoff_zones
on fhv_data.dropoff_locationid = dropoff_zones.locationid
