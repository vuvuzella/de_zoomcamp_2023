{{ config(materialized='view') }}

select

    {{ dbt_utils.generate_surrogate_key(['date_time_id']) }} as id,
    cast(year as int) as year,
    {{ get_int('month') }} as month,
    case
        when day_of_week = 'nan' then null
        else {{ get_int('day_of_week') }}
    end as day_of_week,
    case
        when day_of_month = 'nan' then null
        else {{ get_int('day_of_month') }}
    end as day_of_month,
    {{ get_int('hour') }}as hour,
    cast(approximate as bool) as approximate


from {{ source('anz_road_crash_dataset', 'raw_datetimes') }}
