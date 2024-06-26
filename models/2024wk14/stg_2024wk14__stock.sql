with source as (

    select * from {{ ref('source_pd2024wk14__stock') }}

),

-- there are spellign errors in the source data, identify those errors
city_store_combos as (

    select 

        store_id,
        city,
        store

    from source
    group by store_id, city, store

),

transformed as (

    select *

    from source

)

select * from transformed