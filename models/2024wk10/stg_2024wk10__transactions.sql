-- import CTEs

with source as (

    select * from {{ ref("source_pd2024wk10__transactions") }}

),

-- transformations

transformed as (

    select

        to_date(transaction_date, 'dy, mmmm dd, yyyy') as transaction_date,
        transanction_number as transaction_number,
        lower(product_id) as product_id,
        case
            when cash_or_card = 1
                then 'Card'
            when cash_or_card = 2
                then 'Cash'
        end as cash_or_card,
        loyalty_number,
        sales_before_discount

    from source
    
)

select * from transformed
