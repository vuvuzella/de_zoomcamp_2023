{{ config(materialized='view') }}

with

total_vehicles_per_type_per_state as (

    select
        v.state,
        sum(v.car_sedan) as car_sedan,
        sum(v.car_utility) as car_utility,
        sum(v.car_van) as car_van,
        sum(v.car_4x4) as car_4x4,
        sum(v.car_station_wagon) as car_station_wagon,
        sum(v.motor_cycle) as motor_cycle,
        sum(v.truck_small) as truck_small,
        sum(v.truck_large) as truck_large,
        sum(v.bus) as bus,
        sum(v.taxi) as taxi,
        sum(v.bicycle) as bicycle,
        sum(v.scooter) as scooter,
        sum(v.train) as train,
        sum(v.tram) as tram,

    from {{ ref('int_crashes_vehicles_locations') }} v
    group by v.state
)

select
    *,
    (
        car_sedan +
        car_utility +
        car_van +
        car_4x4 +
        car_station_wagon +
        motor_cycle +
        truck_small +
        truck_large +
        bus +
        taxi +
        bicycle +
        scooter +
        train +
        tram
    ) as TOTAL
from total_vehicles_per_type_per_state t
where t.state is not null and t.state != 'NZ'
