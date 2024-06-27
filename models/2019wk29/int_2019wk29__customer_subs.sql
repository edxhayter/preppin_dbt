-- Import CTEs

with customers as (

    select * from {{ref('stg_2019wk29__customer_packages')}}

),

products as (

    select * from {{ ref('source_pd2019wk29__products')}}

),

schedule as (

    select * from {{ ref('source_pd2019wk29__packages')}}

),

joined as (

    select

        customers.name,
        customers.package,

        products.product,
        products.price,

        schedule.frequency,
        case
            when schedule.frequency = 'week' then 52
            when schedule.frequency = 'month' then 12
            when schedule.frequency = 'quarter' then 4
            else 1
        end as numeric_frequency

    from customers
    inner join schedule on customers.frequency = schedule.subscription_frequency
    inner join products on customers.package = products.subscription_package

)

select * from joined
