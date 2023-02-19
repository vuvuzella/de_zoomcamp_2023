{{ config(materialized='view') }}

with fhv_trip_data as (
    select *
    from {{ source('raw-trip-data', 'raw_fhv_data') }}
)

select
    dispatching_base_num
    ,cast(pickup_datetime as timestamp) as pickup_datetime
    ,cast(dropoff_datetime as timestamp) as dropoff_datetime
    ,cast(pulocationid as integer) as pickup_locationid
    ,cast(dolocationid as integer) as dropoff_locationid
    ,cast(sr_flag as numeric) as sr_flag
    ,affiliated_base_number
from fhv_trip_data

{% if var('is_test_run', default=false) %}

limit 100

{% endif %}
