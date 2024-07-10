with source as (

    select * from {{ ref('source_2021wk27__teams') }}

),

transformed as (

    select

        seed,
        seed as original_pick,
        team
    
    from source

)

select * from transformed