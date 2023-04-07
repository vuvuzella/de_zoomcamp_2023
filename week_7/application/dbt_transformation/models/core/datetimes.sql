{{ config(materialized='view') }}

select

    {{ dbt_utils.generate_surrogate_key(['date_time_id'])}} as id,
    cast(year as int) as year,
    cast(month as int) as month,
    cast(day_of_week as int) as day_of_week,
    case
        when day_of_month = 'nan' then null
        else cast(day_of_month as int)
    end as day_of_month,
    cast(hour as int) as hour,
    cast(approximate as bool) as approximate


from {{ source('anz_road_crash_dataset', 'raw_datetimes') }}
