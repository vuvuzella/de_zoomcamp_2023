{{ config(materialized='view') }}

select
    crashes.id,
    vehicles.car_sedan,
    vehicles.car_utility,
    vehicles.car_van,
    vehicles.car_4x4,
    vehicles.car_station_wagon,
    vehicles.motor_cycle,
    vehicles.truck_small,
    vehicles.truck_large,
    vehicles.bus,
    vehicles.taxi,
    vehicles.bicycle,
    vehicles.scooter,
    vehicles.train,
    vehicles.tram
from {{ ref('stg_crashes') }} crashes
left join {{ ref('stg_vehicles') }} vehicles
on crashes.vehicles_id = vehicles.id
