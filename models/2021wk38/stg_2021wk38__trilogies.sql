with source as (

    select * from {{ ref('source_2021wk38__trilogies')}}

),

transformed as (

    select
    
        trilogy_ranking,
        trim(replace(trilogy, 'trilogy', '')) as trilogy

    from source

)

select * from transformed