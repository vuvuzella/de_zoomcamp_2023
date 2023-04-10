{{ config(materialized='view') }}

select

    {{ dbt_utils.generate_surrogate_key(['vehicles_id']) }} as id,
    {{ get_int('animals') }} as animals,
    {{ get_int('car_sedan') }} as car_sedan,
    {{ get_int('car_utility') }} as car_utility,
    {{ get_int('car_van') }} as car_van,
    {{ get_int('car_4x4') }} as car_4x4,
    {{ get_int('car_station_wagon') }} as car_station_wagon,
    {{ get_int('motor_cycle') }} as motor_cycle,
    {{ get_int('truck_small') }} as truck_small,
    {{ get_int('truck_large') }} as truck_large,
    {{ get_int('bus') }} as bus,
    {{ get_int('taxi') }} as taxi,
    {{ get_int('bicycle') }} as bicycle,
    {{ get_int('scooter') }} as scooter,
    {{ get_int('pedestrian') }} as pedestrian,
    {{ get_int('inanimate') }} as inanimate,
    {{ get_int('train') }} as train,
    {{ get_int('tram') }} as tram,
    {{ get_int('vehicle_other') }} as vehicle_other,

from {{ source('anz_road_crash_dataset', 'raw_vehicles') }}
