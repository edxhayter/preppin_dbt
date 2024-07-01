-- Import CTE

with country_info as (

    select * from {{ ref("stg_2021wk12__tourism") }}
    where stream = 'country'

),

transformed as (

    select 

        date as month,
        region as breakdown,
        area as country,
        tourists as tourists

    from country_info

)

select * from transformed