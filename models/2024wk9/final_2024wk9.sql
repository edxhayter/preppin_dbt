{{ config(materialized='table') }}

-- import cte

with flight_data as (

    select * from {{ ref("int_2024wk9__cumulative_bookings")}}

),

final as (

    select
    flight_number,
    date_of_flight,
    class,
    bookings_over_time,
    capacity,
    zeroifnull(bookings_over_time/capacity) as capacity_pct,
    customer_id,
    action,
    case
        when action_date is null then '2024-02-28'::date
        else action_date
    end as date,
    plane_row,
    seat

    from flight_data

)

select * from final