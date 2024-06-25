with _2024 as (

{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2024-01-01' as date)",
    end_date="cast('2025-01-01' as date)"
   )
}}

),

transformed as (

    select 

        row_number() over (order by (select null)) as id,
        date_day

    from _2024

)

select * from transformed