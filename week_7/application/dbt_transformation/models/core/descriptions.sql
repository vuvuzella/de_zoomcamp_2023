{{ config(materialized='view') }}

select

    cast(description_id as int) as id,
    severity,
    cast(speed_limit as int) as speed_limit,
    cast(midblock as bool) as midblock,
    cast(intersection as bool) as intersection,
    road_position_horizontal,
    road_position_vertical,
    cast(road_sealed as bool) as road_sealed,
    cast(road_wet as bool) as road_wet,
    weather,
    crash_type,
    lighting,
    case
        when traffic_controls = 'none' then null
        else traffic_controls
    end as traffic_controls,
    case
        when drugs_alcohol = 'nan' then null
        else cast(drugs_alcohol as bool)
    end as drugs_alcohol,
    case
        when DCA_code = 'nan' then null
        else DCA_code
    end as DCA_code,
    case
        when comment = 'nan' then null
        else comment
    end as comment

from {{ source('anz_road_crash_dataset', 'raw_descriptions') }}
