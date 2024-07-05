with source as (

    select * from {{ ref("source_2021W25__Evolution_Group") }}

),

transformed as (

    select 

        evolution_group,
        num as dex_num,
        starter_flag,
        legendary_flag,

    from source

)

select * from transformed