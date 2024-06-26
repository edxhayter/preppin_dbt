with recalled as (

    select * from  {{ ref('int_2024wk14__recalled_stock') }}

),

store_costs as (

    select 

        store,
        round(sum(total_cost),2) as total_price_rounded

    from recalled
    group by store        

),

priority as (

    select

        store,
        total_price_rounded,
        case
            when total_price_rounded >= 5000 then 'High Priority'
            else 'Low Priority'
        end as issue_level

    from store_costs

)

select * from priority