-- Import CTEs

with customers as (

    select * from {{ ref('stg_2022wk13__customer_sales') }}

),

pareto as (

    select * from {{ ref('final_2022wk13__pareto') }}

),

total_customers as (

    select
        
        count(distinct customers.customer_id) as customer_count

    from customers

),

filtered_values as (

    select

        count(distinct pareto.customer_id) as filtered_count,
        round(max(running_pct_total_sales)) as pct_sales

    from pareto
),

final as (

    select

        total_customers.customer_count,

        filtered_values.filtered_count,
        filtered_values.pct_sales

    from total_customers
    cross join filtered_values

) 

select 

    (round((filtered_count/customer_count)*100)) || ' % of Customers account for ' || pct_sales || '% of Sales' as Outcome

 from final

 -- Example of DBT Command to Override the Variable

 -- dbt run -s +final_2022wk13__outcome --vars '{filter_value: 50}'