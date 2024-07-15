# Preppin Data 2024 Week 7: Valentine's Day

## Input

This week's challenge has two inputs, a list of couples and when their relationships began and a second input with gift ideas depending on how long a couple have been together.

## Staging

The gifts input only needed one action to be staged turning the year from cardinal (1st, 2nd etc.) to numeric. This was done with a regex replace on any letter characters.

```sql
with source as (

    select * from {{ ref("source_pd2024wk7__gifts") }}

),

transformed as (

    select
        regexp_replace(year, '[a-zA-Z]+', '') as year,
        gift
    from source

)

select * from transformed
```

Staging the relationships file required a date parse and also setting up a flag to mark if their relationship started before or after Valentine's Day in their founding year which will be used in conditional logic in the final model.

```sql
-- import cte

with source as (

    select * from {{ ref("source_pd2024wk7__couples") }}

),

transformed as (

    select

    couple,
    to_date(relationship_start, 'MMMM DD, YYYY') as relationship_date,
    case
            when relationship_date < to_date(left(relationship_date, 4) || '-02-14')
            then 'before'
            else 'after'
    end as before_day

    from source

)

select * from transformed
```

## Final

The final layer involves joining the two tables once a calculation has been made to work out the number of valentines each couple have spent together.

```sql
{{ config(materialized='table') }}

-- import cte's

with couples as (

    select * from {{ ref("stg_2024wk7__couples") }}

),

gifts as (

    select * from {{ ref("stg_2024wk7__gifts") }}

),

-- determine before or after valentines day

couples_prepped as (

    select

        couples.couple,
        datediff(year, couples.relationship_date, '2024-02-14'::date) + case when before_day = 'before' then 1 else 0 end as num_valentines,
        gifts.gift

    from couples
    join gifts on num_valentines = gifts.year

)

select * from couples_prepped
```
