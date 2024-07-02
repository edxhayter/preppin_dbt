with source as (

    select * from {{ ref("source_2021W25__Alolan") }}

),

transformed as (

    select

        name as full_name,
        trim(replace(name, 'Alolan', '')) as poke_name

    from source

)

select * from transformed
