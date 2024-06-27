-- Import the subs table in

with subs as (

    select * from {{ ref('int_2019wk29__customer_subs') }}

),

-- Removing mystery subscriptions work out the price per annum of each subscription

weights as (

    select 
    
        name,
        product,
        min(price)*sum(numeric_frequency) as weighted_price,
        sum(numeric_frequency) as frequency
    
    from subs
    where package != 7
    group by product, name

),

-- to find out the price of the mystery subscription divide the total cost per-annum by the total number of subscriptions made.

mystery_price as (

    select

        floor(sum(weighted_price)/sum(frequency)) as m_price

    from weights
),

-- bring that mystery price into a final table of subscription prices.

sub_prices as (

    select

        subs.package,
        subs.product,
        min(case 
            when subs.product = 'Mystery' then mystery_price.m_price
            else subs.price
        end) as price

    from subs
    cross join mystery_price
    group by package, product

)

select * from sub_prices
