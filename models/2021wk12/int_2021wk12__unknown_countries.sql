-- import CTE

with country_info as (

    select * from {{ ref("stg_2021wk12__tourism") }}
    where stream = 'country'

),

region_info as (

    select * from {{ ref("stg_2021wk12__tourism") }}
    where stream = 'continent'

),

-- logical CTEs
-- Aggregate Country up to the area level
country_to_region as (

    select 

        region,
        date,
        sum(tourists) as known_tourists

    from country_info
    group by region, date

),

-- Join the known region info with the total region info

unknown_region as (

    select

        region_info.area,
        region_info.date as date,
        region_info.tourists as total_tourists,

        country_to_region.known_tourists as known_tourists

    from region_info
    inner join country_to_region on region_info.area = country_to_region.region and region_info.date = country_to_region.date

),

-- make rows for the unknown regional 

unknown_country as (

    select

        date as month,
        area as breakdown,
        'Unknown' as country,
        (total_tourists - zeroifnull(known_tourists)) as tourists

    from unknown_region

)

select * from unknown_country
