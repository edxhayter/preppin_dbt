-- Import CTEs

with gen1 as (

    select * from {{ ref('stg_2021wk25__gen1') }}

),

evo_group as (

    select * from {{ ref('stg_2021wk25__evo_group') }}

),

evo as (

    select * from {{ ref('stg_2021wk25__evos') }}

),

-- Join evogroup to gen 1 mon

gen1_evo as (

    select

        gen1.*,

        evo_group.evolution_group,
        evo_group.starter_flag,
        evo_group.legendary_flag

    from gen1
    join evo_group on gen1.dex_num = evo_group.dex_num

),

-- filter out starters and legendaries

gen1_subset as (

    select

        dex_num,
        poke_name,
        evolution_group

    from gen1_evo
    where starter_flag != 1 and legendary_flag != 1

),

-- join the gen1 data to both evolve from and evolve to - fan out data

evolve_fan_out as (

    select 

        gen1_subset.poke_name,
        gen1_subset.evolution_group,
        
        evo.evolving_to,
        evo.evolving_from

    from evo
    inner join gen1_subset on evo.evolving_from = gen1_subset.poke_name or evo.evolving_to = gen1_subset.poke_name

),

-- filter the fanned out data on a gen1 reference table - only return rows which match to both

remove_post_evo as (

    select 

        evolve_fan_out.*

    from evolve_fan_out
    inner join gen1 on evolve_fan_out.evolving_to = gen1.poke_name

),

remove_pre_evo as (

    select 

        remove_post_evo.*

    from remove_post_evo
    left join gen1 on remove_post_evo.evolving_from = gen1.poke_name

)

select
     
    *

from remove_post_evo

-- union

-- select
    
--     poke_name,
--     evolution_group

-- from post_evolve_gen1
