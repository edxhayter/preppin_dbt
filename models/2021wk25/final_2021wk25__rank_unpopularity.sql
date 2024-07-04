with candidates as (

    select * from {{ ref('int_2021wk25__unpop_no_gimmick') }}

),

appearances as (

    select * from {{ ref('stg_2021wk25__anime') }}

),

joined as (

    select

        candidates.poke_name,
        candidates.evolution_group,

        appearances.episode

    from appearances
    inner join candidates on appearances.pokemon = candidates.poke_name

),

count as (

    select

        evolution_group,
        count(distinct episode) as appearances

    from joined
    group by evolution_group

),

rank as (

    select

        rank() over (order by appearances asc) as worst_pokemon,
        evolution_group,
        appearances

    from count

)

select * from rank