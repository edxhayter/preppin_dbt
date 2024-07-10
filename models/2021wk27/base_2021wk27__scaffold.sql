with source as (

    select * from {{ ref('source_2021wk27__scaffold') }}

)

select * from source