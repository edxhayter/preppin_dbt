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