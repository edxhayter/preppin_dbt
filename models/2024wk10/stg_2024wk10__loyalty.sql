-- import source and mapping

with source as (

    select * from {{ ref("source_pd2024wk10__loyalty") }}

),

mapping_seed as (

    select * from {{ ref("pd_2024wk10__loyalty_mappings")}}

),

-- required transformations on source table

transformed as (

    select 
        
        loyalty_number,
        initcap(split_part(source.customer_name, ',', 2) || ' ' || split_part(source.customer_name, ',', 1)) as customer_name,
        
        -- assumption is that the discounts will stay the same, the loyalty table has some errors in the discount.
        case
            when mapping_seed.standardized_tier = 'Bronze' then 0.05
            when mapping_seed.standardized_tier = 'Silver' then 0.1
            when mapping_seed.standardized_tier = 'Gold' then 0.15
        end as discount,

        mapping_seed.standardized_tier as loyalty_tier

    from source
    join mapping_seed on source.loyalty_tier = mapping_seed.tier

)

select * from transformed