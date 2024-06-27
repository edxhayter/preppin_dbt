-- Import CTEs

with subs as (

    select * from {{ ref('int_2019wk29__customer_subs') }}

),

prices as (

    select * from {{ ref("final_2019wk29__sub_price") }}

),

-- replace sub prices with the price from the final mart table

cost as (

    select

        subs.name,
        subs.package,
        subs.product,
        subs.numeric_frequency,

        prices.price

    from subs
    inner join prices on subs.package = prices.package

),

final as (

    select

        name,
        sum(price*numeric_frequency)

    from cost
    group by name

)

select * from final