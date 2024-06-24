{{ config(materialized='table') }}

-- import cte

with customers as (

    select * from {{ ref("int_2024wk8__customers") }}

),

costings as (

    select * from {{ ref("stg_2024wk8__costings") }}

),

customer_costs as (

    select 
        
        customers.tier,
        customers.customer_id,
        customers.number_of_flights,
        customers.avg_flights_per_year,
       
        costings.cost as cost,  
        costings.per_flight_cost

    from customers
    join costings on customers.benefit = costings.benefit

), 

final as (

    select

        tier,
        sum(case
                when per_flight_cost = true then cost*avg_flights_per_year
                else cost
            end) as yearly_cost,
        count(distinct customer_id) as num_customers

    from customer_costs
    group by tier

)

select * from final