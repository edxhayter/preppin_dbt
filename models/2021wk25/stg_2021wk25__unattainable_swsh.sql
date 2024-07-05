with source as (

    select * from {{ ref('source_2021W25__Unattainable_in_SwSh') }}

)

select * from source