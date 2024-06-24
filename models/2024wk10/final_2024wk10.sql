{{ config(materialized='table') }}

with int_transactions as (

    select * from {{ ref("int_2024wk10__transactions")}}

),

final as (

    select 

        transaction_date,
        transaction_number,
        product_type,
        product_scent,
        product_size,
        cash_or_card,
        loyalty_number,
        customer_name,
        loyalty_tier,
        sales_before_discount/selling_price as quantity,
        sales_before_discount,
        sales_before_discount * (1-discount) as sales_after_discount,
        sales_after_discount - (unit_cost*quantity)as profit

    from int_transactions

)

select * from final
