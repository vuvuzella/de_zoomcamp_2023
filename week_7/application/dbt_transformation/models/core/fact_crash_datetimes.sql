{{ config(materialized='view') }}

select
    crashes.id,
    datetimes.id as datetime_id,
    datetimes.day_of_week as day,
    datetimes.month,
    datetimes.year,
    datetimes.hour
from {{ ref('fact_crashes') }} crashes
left join {{ ref('dim_datetimes') }} datetimes
on crashes.datetime_id = datetimes.id
