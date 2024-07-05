with source as (

    select * from {{ ref('source_2021W25__Evolutions') }}

),

-- assign a rownumber

transformed as (

    select

        row_number() over (order by (select null)) as record_id,
        *
    
    from source

),

pivot as (

    select
        
        record_id, 
        poke_name,
        evo_direction
    
    from transformed
        unpivot(poke_name for evo_direction in (evolving_from, evolving_to))

)

select * from pivot