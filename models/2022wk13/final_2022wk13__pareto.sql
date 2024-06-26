-- need a user value to alter a where clause

{% set filter_value = var('filter_value', 80) %}

-- input CTEs

with sales_pct as (

    select * from {{ ref('stg_2022wk13__customer_sales') }}

),

-- add the running sum of pct

running_sales as (

    select

       customer_id,
       first_name,
       surname,
       sales_by_customer as sales,
       pct_of_total,
       round(sum(pct_of_total) over (order by pct_of_total desc), 2) as running_pct_total_sales

    from sales_pct

    order by pct_of_total desc



)

select * from running_sales
where running_pct_total_sales <= {{ var("filter_value") }}