with source as (

    select * from {{ ref("source_pd2024wk8__customers") }}

),

transformed as (

    select 
        customer_id,
        first_name,
        last_name,
        email,
        gender,
        first_flight,
        last_date_flown,
        number_of_flights,
        case 
             when datediff(days, cast('2023-02-21' as date), last_date_flown) >= 0 then 1
             else 0 
        end as flown_in_past_year,
        datediff(years, first_flight, last_date_flown) + 1 as years_customer

    from source

)

select * from transformed 