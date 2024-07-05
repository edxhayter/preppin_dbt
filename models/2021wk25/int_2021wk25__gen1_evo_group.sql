-- Import CTEs

with gen1 as (

    select * from {{ ref('stg_2021wk25__gen1') }}

),

evo_group as (

    select * from {{ ref('stg_2021wk25__evo_group') }}

),

final as (

    select

        gen1.poke_name,
        gen1.dex_num,

        evo_group.evolution_group,
        evo_group.starter_flag,
        evo_group.legendary_flag

    from gen1
    inner join evo_group on gen1.dex_num = evo_group.dex_num

)

select * from final