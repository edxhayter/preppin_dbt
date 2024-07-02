-- Import CTEs

with unpop as (

    select * from {{ ref('int_2021wk25__unpop_pokemon') }}

),

gimmick as (

    select * from {{ ref('int_2021wk25__gimmick_exclusions') }}

),

unattainable as (

    select * from {{ ref('stg_2021wk25__unattainable_swsh') }}

),

-- exclude gimmicks

unpop_no_gimmick as (

    select 

        unpop.*,

        gimmick.gimmick_flag

    from unpop
    left join gimmick on unpop.poke_name = gimmick.poke_name
    where gimmick_flag is null

),

-- exclude attainable

unpop_unattainable as (

    select

        unpop_no_gimmick.evolution_group

    from unpop_no_gimmick
    inner join unattainable on unpop_no_gimmick.poke_name = unattainable.name
    

)

select * from unpop_unattainable
