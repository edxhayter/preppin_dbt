-- this model is to be run with the the --vars command to specify bin size.

{% set bin_size = var('bin_size', 5) %}

-- import cte

with passengers as (

    select 

        customer_id,
        years_customer,
        number_of_flights
    
    from {{ ref("stg_2024wk8__customers") }}
    where flown_in_past_year = 1

),

loyalty_benefits as (

    select * 
    from {{ ref("stg_2024wk8__loyalty") }}
    where bin_size = {{ bin_size }}

),

-- logical transformations

loyalty_tiers as (

    select

    customer_id,
    floor(number_of_flights / {{ bin_size }}) as tier,
    number_of_flights / years_customer as avg_flights_per_year

    from passengers

),

final as (

    select 
        
        loyalty_tiers.*,

        loyalty_benefits.number_of_flights,
        loyalty_benefits.tier as benefit_tier,
        replace(benefits.value, '"', '') as benefit

    from loyalty_tiers
    
    left join loyalty_benefits on loyalty_benefits.tier <= loyalty_tiers.tier,
    lateral flatten(input => split(loyalty_benefits.benefits, ',')) as benefits
    
    where benefit != ' '

)

select * from final