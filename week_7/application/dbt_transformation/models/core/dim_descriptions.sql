{{ config(materialized='view') }}

select

    {{ dbt_utils.generate_surrogate_key(['description_id']) }} as id,
    severity,
    {{ get_int('speed_limit') }} as speed_limit,
    cast(midblock as bool) as midblock,
    cast(intersection as bool) as intersection,
    case
        when road_position_horizontal = 'nan' then null
        else cast(road_position_horizontal as string)
    end as road_position_horizontal,
    case
        when road_position_vertical = 'nan' then null
        else cast(road_position_vertical as string)
    end as road_position_vertical,
    weather,
    crash_type,
    lighting,
    case
        when road_wet = 'nan' then null
        when road_wet = 'Y' then True
        when road_wet = 'N' then False
        else cast(road_wet as bool)
    end as road_wet,
    case
        when road_sealed = 'nan' then null
        when road_sealed = 'Y' then True
        when road_sealed = 'N' then False
        else cast(road_sealed as bool)
    end as road_sealed,
    case
        when traffic_controls = 'none' then null
        else traffic_controls
    end as traffic_controls,
    case
        when drugs_alcohol = 'nan' then null
        when drugs_alcohol = 'Y' then True
        when drugs_alcohol = 'N' then False
        else cast(drugs_alcohol as bool)
    end as drugs_alcohol,
    case
        when DCA_code = 'nan' then null
        else {{ get_int('DCA_code') }}
    end as DCA_code,
    case
        when comment = 'nan' then null
        else comment
    end as comment

from {{ source('anz_road_crash_dataset', 'raw_descriptions') }}
