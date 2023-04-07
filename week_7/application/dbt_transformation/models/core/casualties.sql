{{ config(materialized='view') }}

select

    cast(casualties_id as string) as id,
    cast(casualties as int) as casualties,
    cast(fatalities as int) as fatalities,
    cast(serious_injuries as int) as serious_injuries,
    cast(minor_injuries as int) as minor_injuries,

from {{ source('anz_road_crash_dataset', 'raw_casualties') }}
