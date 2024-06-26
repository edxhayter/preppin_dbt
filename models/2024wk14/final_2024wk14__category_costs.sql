-- import CTE

with recalled as (

    select * from {{ ref('int_2024wk14__recalled_stock') }}

),

category_cost as (

    select

        category,
        round(sum(total_cost),2) as total_price_rounded

    from recalled
    group by category

)

select * from category_cost