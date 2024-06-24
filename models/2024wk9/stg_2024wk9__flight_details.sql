with source as (

    select * from {{ ref("source_pd2024wk9__flight_details") }}

),

transformed as (

    select 

        flight_number,
        to_date(flight_date, 'dd/mm/yyyy') as date_of_flight,
        class,
        capacity

    from source

)

select * from transformed