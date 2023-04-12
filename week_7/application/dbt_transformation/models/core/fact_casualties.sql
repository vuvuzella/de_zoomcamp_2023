{{ config(materialized='view') }}

select

    {{ dbt_utils.generate_surrogate_key(['casualties_id']) }} as id,
    {{ get_int('casualties') }} as casualties,
    {{ get_int('fatalities') }} as fatalities,
    {{ get_int('serious_injuries') }} as serious_injuries,
    {{ get_int('minor_injuries') }} as minor_injuries,

from {{ source('anz_road_crash_dataset', 'raw_casualties') }}
