{{ config(materialized='view') }}

with

crashes_casualties as (

    select
        crashes.id,
        casualties.casualties,
        casualties.fatalities,
        casualties.minor_injuries,
        casualties.serious_injuries
    from {{ ref('stg_crashes') }} crashes
    left join {{ ref('stg_casualties') }} casualties
    on crashes.casualties_id = casualties.id

)

select
    crashes_casualties.*,
    v.car_sedan,
    v.car_utility,
    v.car_van,
    v.car_4x4,
    v.car_station_wagon,
    v.motor_cycle,
    v.truck_small,
    v.truck_large,
    v.bus,
    v.taxi,
    v.bicycle,
    v.scooter,
    v.train,
    v.tram
from crashes_casualties
left join {{ ref('int_crashes_vehicles') }} v
on crashes_casualties.id = v.id


