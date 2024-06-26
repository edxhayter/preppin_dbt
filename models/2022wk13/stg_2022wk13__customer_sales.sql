with source as (

    select * from {{ ref('source_2022wk13__pareto') }}

),

customer_names as (

    select

        customer_id,
        first_name,
        surname

    from source
    group by customer_id, first_name, surname

),

total_sales as (

    select

        sum(sales) as all_sales

    from source

),

customer_sales as (

    select

        customer_id,
        sum(sales) as sales_by_customer

    from source
    group by customer_id

),

transformed as (

    select 

    customer_sales.customer_id,
    customer_sales.sales_by_customer,
    (customer_sales.sales_by_customer/total_sales.all_sales)*100 as pct_of_total,

    customer_names.first_name,
    customer_names.surname

    from customer_sales
    join customer_names on customer_names.customer_id = customer_sales.customer_id
    cross join total_sales

)

select * from transformed