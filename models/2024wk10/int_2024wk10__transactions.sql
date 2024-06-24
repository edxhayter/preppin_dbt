-- import CTEs

with transactions as (

    select * from {{ ref("stg_2024wk10__transactions") }}

),

date_scaffold as (

    select * from {{ ref("__2023_2024_date_scaffold") }}

),

product as (

    select * from {{ ref("stg_2024wk10_products") }}

),

loyalty as (

    select * from {{ ref("stg_2024wk10__loyalty") }}

),

-- add rows for days the store was closed

all_days as (

    select 
    
        transactions.*,
        
        date_scaffold.transaction_date as date

    from date_scaffold
    left join transactions on date_scaffold.transaction_date = transactions.transaction_date
),

sales_after_discount as (

    select 
    
        all_days.transaction_date,
        all_days.transaction_number,
        all_days.cash_or_card,
        all_days.loyalty_number,
        all_days.sales_before_discount,

        product.product_type,
        product.product_scent,
        product.product_size,
        product.selling_price,
        product.unit_cost,

        loyalty.customer_name,
        loyalty.loyalty_tier,
        loyalty.discount

    from all_days
    join product on all_days.product_id = product.join_term
    left join loyalty on all_days.loyalty_number = loyalty.loyalty_number

)

select * from sales_after_discount