-- Import CTEs
with gen1 as (

    select * from {{ ref('int_2021wk25__gen1_evo_group') }}

),

unpop as (

    select * from {{ ref('int_2021wk25__unpop_pokemon') }}

),

gimmick as (

    select * from {{ ref('int_2021wk25__gimmick_evo_group') }}

),

unattainable as (

    select * from {{ ref('stg_2021wk25__unattainable_swsh') }}

),

-- exclude gimmicks

gen1_no_gimmick as (

    select 

        gen1.poke_name,
        gen1.evolution_group

    from gen1
    left outer join gimmick on gen1.evolution_group = gimmick.evolution_group
    where gimmick.evolution_group is null

),

-- exclude attainable

gen1_unattainable as (

    select

        gen1_no_gimmick.evolution_group

    from gen1_no_gimmick
    inner join unattainable on gen1_no_gimmick.poke_name = unattainable.name
    

),

-- exclude the not allowed evos

final_groups as (

    select

        gen1_unattainable.evolution_group

    from gen1_unattainable
    inner join unpop on gen1_unattainable.evolution_group = unpop.evolution_group

),

-- join the final groups back to the pokemon level table

final as (

    select

        gen1.poke_name,
        gen1.evolution_group

    from gen1
    inner join final_groups on gen1.evolution_group = final_groups.evolution_group

)

select * from final
