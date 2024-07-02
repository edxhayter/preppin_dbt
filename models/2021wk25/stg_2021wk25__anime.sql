with source as (

    select * from {{ ref('source_2021W25__Anime') }}

)

select * from source