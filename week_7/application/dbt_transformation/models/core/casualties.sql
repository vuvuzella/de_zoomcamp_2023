{{ config(materialized='view') }}

select

    {{ dbt_utils.generate_surrogate_key(['casualties_id']) }} as id,
    cast(casualties as float64) as casualties,
    cast(fatalities as float64) as fatalities,
    cast(serious_injuries as float64) as serious_injuries,
    cast(minor_injuries as float64) as minor_injuries,

from {{ source('anz_road_crash_dataset', 'raw_casualties') }}
