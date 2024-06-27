### 2019 Wk 29: C&BS Co: Subscription Packages

## Inputs (Sources)

![Customer Table](image.png)

Customer Table: 1 Row per Customer Detailing the Subscriptions the individual has and the frequency they have them delivered.

![Subscriptions Table](image-1.png)

Subscriptions Table: 1 row for each subscription and the price it costs to produce once

![Frequency Table](image-2.png)

Frequency Table: 1 row for each frequency converting a number to the frequency in date terms (weekly, monthly, yearly etc.)

## Requirements

- Input the data file
- Calculate the price of the mystery package **_Jonathan rounded the price down to provide value of money for Chin & Beard Suds Co customers_**
- Join back to original table containing product aliases and prices
- Calculate total cost of each customers subscription normalised on an annual basis
- Output the file

## DBT Actions

1. Read the data in using 'dbt seed'. Seeds make sense for static reference data and therefore make sense for the subscription mapping table.
   - Ideally the customers table and the price table would likely change over time in the real world. As such it would be more beneficial to set up source tables where freshness checks can be set up and snapshot methods can be used to capture slowly changing dimensions.
2. Staged the customer table using a lateral join to a flattened version of the table where we split on the '|'.

```sql

with source as (

    select * from {{ ref('source_pd2019wk29__customers') }}

),

-- split the package value into multiple rows within query using a lateral join to a flattened version of the file that splits based on the | delimeter.

transformed as (

    select

        name,
        package.value::int as package,
        frequency

    from source,
    lateral flatten(split(packages, '|')) package

)

select * from transformed
```

3. Next I joined the various table together to build an itermediate model with all the required information at the granularity of a row for each customer subscription combination. Including a value for how many times the subscription is delivered in a year.

```sql

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

```

4. Built the first of the two final models a price table that uses a weighted calculation to price the mystery subscription that currently has no value assigned. This model uses where clauses to remove the mystery rows. It then aggregates both the price and the total subscriptions sent out.

Finally some join logic is used to replace the old price value of nothing for the mystery subscription with the calculated value.

```sql

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

```

5. Finally we needed to calculate the yearly cost each customers subscriptions come to. This involved using the other final table that has a complete price list (rather than a missing value for mystery). Joining that price list on appropriately and then aggregating the information up to the customer level.

```sql

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

```

The final outputs were as expected.
