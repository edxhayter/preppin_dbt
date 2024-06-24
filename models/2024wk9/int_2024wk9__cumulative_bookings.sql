-- import cte

with bookings as (

    select * from {{ ref( "stg_2024wk9__customer_actions")}}

),

flight_details as (

    select * from {{ ref("stg_2024wk9__flight_details") }}

),

-- cumulative number of bookings

cumulative_bookings as (

    select  

        flight_number,
        action_date,
        customer_id,
        action,
        date_of_flight,
        class,
        plane_row,
        seat,
        count(customer_id) over (partition by flight_number, class order by action_date asc) as bookings_over_time

    from bookings

),

--bring in flight details information
-- left join booking info on so that we have rows for flight class combos with no bookings.

flight_data as (

    select 

        flight_details.flight_number,
        flight_details.date_of_flight,
        flight_details.class,
        flight_details.capacity,

        cumulative_bookings.customer_id,
        cumulative_bookings.action,
        cumulative_bookings.action_date,
        cumulative_bookings.plane_row,
        cumulative_bookings.seat,
        cumulative_bookings.bookings_over_time

    from flight_details
    left join cumulative_bookings 
        on flight_details.flight_number = cumulative_bookings.flight_number
        and flight_details.class = cumulative_bookings.class

)

select * from flight_data