{{ config(materialized='view') }}

select
    type,
    total
from
    (
        select
            'car_sedan' as type,
            sum(car_sedan) as total,
            -- sum(car_utility) as car_utility,
            -- sum(car_van) as car_van,
            -- sum(car_4x4) as car_4x4,
            -- sum(car_station_wagon) as car_station_wagon,
            -- sum(bicycle) as bicycle,
            -- sum(motor_cycle) as motor_cycle,
            -- sum(scooter) as scooter,
            -- sum(truck_small) as truck_small,
            -- sum(truck_large) as truck_large,
            -- sum(bus) as bus,
            -- sum(taxi) as taxi,
            -- sum(train) as train,
            -- sum(tram) as tram,
            -- sum(pedestrian) as pedestrian,
            -- sum(animals) as animals,
            -- sum(inanimate) as inanimate,
            -- sum(vehicle_other) as vehicle_other

        from {{ ref('stg_vehicles') }} vehicles
    )

union all
    (

    select
        'car_utility' as type,
        sum(car_utility) as total,
        -- sum(car_van) as car_van,
        -- sum(car_4x4) as car_4x4,
        -- sum(car_station_wagon) as car_station_wagon,
        -- sum(bicycle) as bicycle,
        -- sum(motor_cycle) as motor_cycle,
        -- sum(scooter) as scooter,
        -- sum(truck_small) as truck_small,
        -- sum(truck_large) as truck_large,
        -- sum(bus) as bus,
        -- sum(taxi) as taxi,
        -- sum(train) as train,
        -- sum(tram) as tram,
        -- sum(pedestrian) as pedestrian,
        -- sum(animals) as animals,
        -- sum(inanimate) as inanimate,
        -- sum(vehicle_other) as vehicle_other

        from {{ ref('stg_vehicles') }} vehicles
    )

union all
    (
        select
            'car_van' as type,
            sum(car_van) as total,
            -- sum(car_4x4) as car_4x4,
            -- sum(car_station_wagon) as car_station_wagon,
            -- sum(bicycle) as bicycle,
            -- sum(motor_cycle) as motor_cycle,
            -- sum(scooter) as scooter,
            -- sum(truck_small) as truck_small,
            -- sum(truck_large) as truck_large,
            -- sum(bus) as bus,
            -- sum(taxi) as taxi,
            -- sum(train) as train,
            -- sum(tram) as tram,
            -- sum(pedestrian) as pedestrian,
            -- sum(animals) as animals,
            -- sum(inanimate) as inanimate,
            -- sum(vehicle_other) as vehicle_other
        from {{ ref('stg_vehicles') }} vehicles
    )


union all
    (
        select
            'car_4x4' as type,
            sum(car_4x4) as total,
            -- sum(car_station_wagon) as car_station_wagon,
            -- sum(bicycle) as bicycle,
            -- sum(motor_cycle) as motor_cycle,
            -- sum(scooter) as scooter,
            -- sum(truck_small) as truck_small,
            -- sum(truck_large) as truck_large,
            -- sum(bus) as bus,
            -- sum(taxi) as taxi,
            -- sum(train) as train,
            -- sum(tram) as tram,
            -- sum(pedestrian) as pedestrian,
            -- sum(animals) as animals,
            -- sum(inanimate) as inanimate,
            -- sum(vehicle_other) as vehicle_other
        from {{ ref('stg_vehicles') }} vehicles
    )

union all
    (
        select
            'car_station_wagon' as type,
            sum(car_station_wagon) as total,
            -- sum(bicycle) as bicycle,
            -- sum(motor_cycle) as motor_cycle,
            -- sum(scooter) as scooter,
            -- sum(truck_small) as truck_small,
            -- sum(truck_large) as truck_large,
            -- sum(bus) as bus,
            -- sum(taxi) as taxi,
            -- sum(train) as train,
            -- sum(tram) as tram,
            -- sum(pedestrian) as pedestrian,
            -- sum(animals) as animals,
            -- sum(inanimate) as inanimate,
            -- sum(vehicle_other) as vehicle_other
        from {{ ref('stg_vehicles') }} vehicles
    )

union all
    (
        select
            'bicycle' as type,
            sum(bicycle) as total,
            -- sum(motor_cycle) as motor_cycle,
            -- sum(scooter) as scooter,
            -- sum(truck_small) as truck_small,
            -- sum(truck_large) as truck_large,
            -- sum(bus) as bus,
            -- sum(taxi) as taxi,
            -- sum(train) as train,
            -- sum(tram) as tram,
            -- sum(pedestrian) as pedestrian,
            -- sum(animals) as animals,
            -- sum(inanimate) as inanimate,
            -- sum(vehicle_other) as vehicle_other
        from {{ ref('stg_vehicles') }} vehicles
    )

union all
    (
        select
            'motor_cycle' as type,
            sum(motor_cycle) as total,
            -- sum(scooter) as scooter,
            -- sum(truck_small) as truck_small,
            -- sum(truck_large) as truck_large,
            -- sum(bus) as bus,
            -- sum(taxi) as taxi,
            -- sum(train) as train,
            -- sum(tram) as tram,
            -- sum(pedestrian) as pedestrian,
            -- sum(animals) as animals,
            -- sum(inanimate) as inanimate,
            -- sum(vehicle_other) as vehicle_other
        from {{ ref('stg_vehicles') }} vehicles
    )

union all
    (
        select
            'scooter' as type,
            sum(scooter) as total,
            -- sum(truck_small) as truck_small,
            -- sum(truck_large) as truck_large,
            -- sum(bus) as bus,
            -- sum(taxi) as taxi,
            -- sum(train) as train,
            -- sum(tram) as tram,
            -- sum(pedestrian) as pedestrian,
            -- sum(animals) as animals,
            -- sum(inanimate) as inanimate,
            -- sum(vehicle_other) as vehicle_other
        from {{ ref('stg_vehicles') }} vehicles
    )

union all
    (
        select
            'truck_small' as type,
            sum(truck_small) as total,
            -- sum(truck_large) as truck_large,
            -- sum(bus) as bus,
            -- sum(taxi) as taxi,
            -- sum(train) as train,
            -- sum(tram) as tram,
            -- sum(pedestrian) as pedestrian,
            -- sum(animals) as animals,
            -- sum(inanimate) as inanimate,
            -- sum(vehicle_other) as vehicle_other
        from {{ ref('stg_vehicles') }} vehicles
    )

union all
    (
        select
            'truck_large' as type,
            sum(truck_large) as total,
            -- sum(bus) as bus,
            -- sum(taxi) as taxi,
            -- sum(train) as train,
            -- sum(tram) as tram,
            -- sum(pedestrian) as pedestrian,
            -- sum(animals) as animals,
            -- sum(inanimate) as inanimate,
            -- sum(vehicle_other) as vehicle_other
        from {{ ref('stg_vehicles') }} vehicles
    )

union all
    (
        select
            'total' as type,
            sum(bus) as total,
            -- sum(taxi) as taxi,
            -- sum(train) as train,
            -- sum(tram) as tram,
            -- sum(pedestrian) as pedestrian,
            -- sum(animals) as animals,
            -- sum(inanimate) as inanimate,
            -- sum(vehicle_other) as vehicle_other
        from {{ ref('stg_vehicles') }} vehicles
    )

union all
    (
        select
            'taxi' as type,
            sum(taxi) as total,
            -- sum(train) as train,
            -- sum(tram) as tram,
            -- sum(pedestrian) as pedestrian,
            -- sum(animals) as animals,
            -- sum(inanimate) as inanimate,
            -- sum(vehicle_other) as vehicle_other
        from {{ ref('stg_vehicles') }} vehicles
    )

union all
    (
        select
            'train' as type,
            sum(train) as total,
            -- sum(tram) as tram,
            -- sum(pedestrian) as pedestrian,
            -- sum(animals) as animals,
            -- sum(inanimate) as inanimate,
            -- sum(vehicle_other) as vehicle_other
        from {{ ref('stg_vehicles') }} vehicles
    )

union all
    (
        select
            'tram' as type,
            sum(tram) as total,
            -- sum(pedestrian) as pedestrian,
            -- sum(animals) as animals,
            -- sum(inanimate) as inanimate,
            -- sum(vehicle_other) as vehicle_other
        from {{ ref('stg_vehicles') }} vehicles
    )

union all
    (
        select
            'pedestrian' as type,
            sum(pedestrian) as total,
            -- sum(animals) as animals,
            -- sum(inanimate) as inanimate,
            -- sum(vehicle_other) as vehicle_other
        from {{ ref('stg_vehicles') }} vehicles
    )

union all
    (
        select
            'animals' as type,
            sum(animals) as total,
            -- sum(inanimate) as inanimate,
            -- sum(vehicle_other) as vehicle_other
        from {{ ref('stg_vehicles') }} vehicles
    )

union all
    (
        select
            'inanimate' as type,
            sum(inanimate) as inanimate,
            -- sum(vehicle_other) as vehicle_other
        from {{ ref('stg_vehicles') }} vehicles
    )

union all
    (
        select
            'vehicle_other' as type,
            sum(vehicle_other) as vehicle_other
        from {{ ref('stg_vehicles') }} vehicles
    )
