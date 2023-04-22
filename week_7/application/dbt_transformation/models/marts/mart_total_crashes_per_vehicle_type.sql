{{ config(materialized='view') }}

-- TODO: use jinja loop construct for this. set a list of column names and iterate for each of them

with au_crashes as (
    select crashes.*
    from {{ ref('stg_crashes') }} crashes
    left join {{ ref('int_crashes_locations')}} loc
    on crashes.location_id = loc.id
    where loc.country = 'AU'
)

, au_crash_vehicles as (
    select
        vehicles.*
    from au_crashes
    inner join {{ ref('stg_vehicles') }} vehicles
    on au_crashes.vehicles_id = vehicles.id
)

select
    type,
    total
from
    (
        select
            'car_sedan' as type,
            sum(car_sedan) as total,
        from au_crash_vehicles
    )

union all
    (

    select
        'car_utility' as type,
        sum(car_utility) as total,
        from au_crash_vehicles
    )

union all
    (
        select
            'car_van' as type,
            sum(car_van) as total,
        from au_crash_vehicles
    )


union all
    (
        select
            'car_4x4' as type,
            sum(car_4x4) as total,
        from au_crash_vehicles
    )

union all
    (
        select
            'car_station_wagon' as type,
            sum(car_station_wagon) as total,
        from au_crash_vehicles
    )

union all
    (
        select
            'bicycle' as type,
            sum(bicycle) as total,
        from au_crash_vehicles
    )

union all
    (
        select
            'motor_cycle' as type,
            sum(motor_cycle) as total,
        from au_crash_vehicles
    )

union all
    (
        select
            'scooter' as type,
            sum(scooter) as total,
        from au_crash_vehicles
    )

union all
    (
        select
            'truck_small' as type,
            sum(truck_small) as total,
        from au_crash_vehicles
    )

union all
    (
        select
            'truck_large' as type,
            sum(truck_large) as total,
        from au_crash_vehicles
    )

union all
    (
        select
            'total' as type,
            sum(bus) as total,
        from au_crash_vehicles
    )

union all
    (
        select
            'taxi' as type,
            sum(taxi) as total,
        from au_crash_vehicles
    )

union all
    (
        select
            'train' as type,
            sum(train) as total,
        from au_crash_vehicles
    )

union all
    (
        select
            'tram' as type,
            sum(tram) as total,
        from au_crash_vehicles
    )

union all
    (
        select
            'pedestrian' as type,
            sum(pedestrian) as total,
        from au_crash_vehicles
    )

union all
    (
        select
            'animals' as type,
            sum(animals) as total,
        from au_crash_vehicles
    )

union all
    (
        select
            'inanimate' as type,
            sum(inanimate) as inanimate,
        from au_crash_vehicles
    )

union all
    (
        select
            'vehicle_other' as type,
            sum(vehicle_other) as vehicle_other
        from au_crash_vehicles
    )
