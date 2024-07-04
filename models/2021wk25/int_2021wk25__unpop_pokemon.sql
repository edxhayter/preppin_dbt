-- Import CTEs

with gen1 as (

    select * from {{ ref("int_2021wk25__gen1_evo_group") }}

),

evo as (

    select * from {{ ref('int_2021wk25__remove_ineligible_evos') }}

),

-- filter out starters and legendaries

gen1_subset as (

    select

        dex_num,
        poke_name,
        evolution_group

    from gen1
    where starter_flag != 1 and legendary_flag != 1

),

-- join the gen1 data to both evolve from and evolve to - fan out data

evolve_fan_out as (

    select 

        gen1_subset.poke_name,
        gen1_subset.evolution_group,
        
        evo.evolves_to,
        evo.evolves_from,
        evo.flag

    from evo
    left join gen1_subset on evo.evolves_from = gen1_subset.poke_name or evo.evolves_to = gen1_subset.poke_name

),

-- aggregate the table to evolution group
-- then filter out where max(flag) is 1, leaving a list of evolution groups to focus on.

final as (

    select
        
        evolution_group

    from evolve_fan_out
    group by evolution_group
    having max(flag) < 1

)

select * from final
