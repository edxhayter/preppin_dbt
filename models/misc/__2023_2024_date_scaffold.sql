with generator as (

{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2023-01-01' as date)",
    end_date="cast('2025-01-01' as date)"
   )
}}

),


transformed as (

    select

        date_day as transaction_date
    
    from generator

)

select * from transformed
