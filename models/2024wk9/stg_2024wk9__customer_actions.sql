with source as (

    select * from {{ ref("source_pd2024wk9__customer_actions") }}

),

cancellations as (

    select 

        customer_id,
        flight_number,
        1 as filter_field

    from source
    where action = 'Cancelled'    

),

remove_cancellations as (

    select 
    
    source.*,

    cancellations.filter_field,
    case 
        when date = max(date) over (partition by source.flight_number, source.customer_id)
        then 1
        else 0
    end as recent_action

    from source
    left outer join cancellations 
        on source.customer_id = cancellations.customer_id
        and source.flight_number = cancellations.flight_number

    where filter_field is null
),

recent_action as (

    select *

    from remove_cancellations

    where recent_action = 1

),

transformed as (

    select

        flight_number,
        to_date(flight_date, 'dd/mm/yyyy') as date_of_flight,
        customer_id,
        action,
        to_date(date, 'dd/mm/yyyy') as action_date,
        class,
        plane_row,
        seat

    from recent_action

)

select * from transformed