{{ config(materialized='view') }}

select
    crashes.id,

    vehicles.id as vehicle_id,
    vehicles.car_sedan,
    vehicles.car_utility,
    vehicles.car_van,
    vehicles.car_4x4,
    vehicles.car_station_wagon,
    vehicles.bicycle,
    vehicles.motor_cycle,
    vehicles.scooter,
    vehicles.truck_small,
    vehicles.truck_large,
    vehicles.bus,
    vehicles.taxi,
    vehicles.train,
    vehicles.tram,
    vehicles.pedestrian,
    vehicles.animals,
    vehicles.inanimate,
    vehicles.vehicle_other

from {{ ref('fact_crashes') }} crashes
left join {{ ref('fact_vehicles') }} vehicles
on crashes.vehicles_id = vehicles.id
