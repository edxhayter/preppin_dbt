with source as (

    select * from {{ ref("source_pd2024wk8__costings") }}

),

transformed as (

    select 

        benefit,
        contains(cost, 'per flight') as per_flight_cost,
        regexp_substr(cost, '\\d+') as cost

    from source

)

select * from transformed