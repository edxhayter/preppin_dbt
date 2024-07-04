-- Import the gimmick list and the pokemon to evogroup lookup
with gimmicks as (

    select * from {{ ref('int_2021wk25__gimmick_exclusions') }}

),

evo_group_lookup as (

    select * from {{ ref('int_2021wk25__gen1_evo_group') }}

),

-- Summarize the joined table on evo_group to get a list of evo_groups to exclude from the unpopular pokemon finalists.

final as (

    select

        evo_group_lookup.evolution_group
    
    from gimmicks
    inner join evo_group_lookup on gimmicks.poke_name = evo_group_lookup.poke_name
    group by evo_group_lookup.evolution_group

)

select * from final