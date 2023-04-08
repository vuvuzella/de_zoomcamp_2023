{{ config(materialized='view') }}

select
    {{ dbt_utils.generate_surrogate_key(['vehicles_id']) }} as id,
    cast(animals as int) as animals,
    cast(car_sedan as int) as car_sedan,
    cast(car_utility as int) as car_utility,
    cast(car_van as int) as car_van,
    cast(car_4x4 as int) as car_4x4,
    cast(car_station_wagon as int) as car_station_wagon,
    cast(motor_cycle as int) as motor_cycle,
    cast(truck_small as int) as truck_small,
    cast(truck_large as int) as truck_large,
    cast(bus as int) as bus,
    cast(taxi as int) as taxi,
    cast(bicycle as int) as bicycle,
    cast(scooter as int) as scooter,
    cast(pedestrian as int) as pedestrian,
    cast(inanimate as int) as inanimate,
    cast(train as int) as train,
    cast(tram as int) as tram,
    cast(vehicle_other as int) as vehicle_other,
from {{ source('anz_road_crash_dataset', 'raw_vehicles') }}
