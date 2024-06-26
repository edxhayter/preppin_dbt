-- set week start to monday for calcs
{{ config(
  sql_header="alter session set week_start = 1;"
) }}


with sales as (

    select * from {{ ref('stg_2024wk13__sales') }}

),

week_nums as (

    select

        sale_date,
        dense_rank() over (partition by year(sale_date) order by week(sale_date) asc) as week_number,
        case
            when dayname(sale_date) in ('Sat', 'Sun', 'Tue', 'Thu') then left(dayname(sale_date), 2)
            else left(dayname(sale_date), 1)
        end as day,
        date_part('dow', sale_date) as day_order,
        product,
        price,
        quantity
    
    from sales

)

select * from week_nums