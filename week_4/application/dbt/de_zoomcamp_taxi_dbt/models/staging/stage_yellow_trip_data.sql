{{
    config(materialized='view')
}}

with trip_data as (
    -- Deduplication to make tripid uniqueness test pass
    select
        *
        -- row_number assign a rank number
        -- over is a window function, defined by partition by and order by
        -- partition by groups similar values, but does not reduce them
        -- this means for each grouping created in the partition by clause, assign a number to them
        -- This can be used to deduplicate by getting only the rank 1 (or any number really, we just want to get 1 row from the set of rows in that partition)
        , row_number() over(partition by vendorid, tpep_pickup_datetime) as rn
    from {{ source('raw-trip-data', 'yellow_taxi_data') }}
    where vendorid is not null
)

select
    -- identifiers
    {{ dbt_utils.surrogate_key([ 'vendorid', 'tpep_pickup_datetime' ]) }} as tripid,
    cast(vendorid as integer) as vendorid,
    cast(ratecodeid as integer) as ratecodeid,
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,

    -- timestamps
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    1 as trip_type, -- yellow cab always street hail trips

    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    0 as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as integer) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description,
    cast(congestion_surcharge as numeric) as congestion_surcharge
from trip_data
where rn = 1    -- get 1 row from the set of rows created by the pratition by

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

limit 100

{% endif %}
