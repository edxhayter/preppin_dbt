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