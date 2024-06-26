-- input CTEs

with items as (

    select * from {{ ref('stg_2024wk14__recalled_items') }}

),

stock as (

    select * from {{ ref('stg_2024wk14__stock') }}

),

recalled as (

    select 
    
        stock.*,
        (stock.unit_price * stock.quantity) as total_cost

    from stock
    inner join items on stock.product_id = items.product_id

)

select * from recalled