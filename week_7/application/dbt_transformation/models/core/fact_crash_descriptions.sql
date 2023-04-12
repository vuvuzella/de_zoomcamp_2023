{{ config(materialized='view') }}

select

    crashes.id,
    descriptions.id as description_id,
    descriptions.speed_limit,
    descriptions.midblock,
    descriptions.intersection,
    descriptions.road_position_horizontal,
    descriptions.road_position_vertical,
    descriptions.weather,
    descriptions.crash_type,
    descriptions.lighting,
    descriptions.road_wet,
    descriptions.road_sealed,
    descriptions.traffic_controls,
    descriptions.drugs_alcohol,
    descriptions.DCA_code,
    descriptions.comment,

from {{ ref('fact_crashes') }} as crashes
left join {{ ref('dim_descriptions') }} descriptions
on crashes.description_id = descriptions.id
