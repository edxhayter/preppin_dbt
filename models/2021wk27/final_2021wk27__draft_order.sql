-- import cte

with first_4 as (

    select * from {{ ref('int_2021wk27__picks') }}

),

teams as (

    select * from {{ ref('stg_2021wk27__teams') }}

),

-- left outer join for unpicked teams

undrawn_teams as (

    select

        seq4() + 5 as pick,
        teams.seed
    
    from teams
    left join first_4 on teams.original_pick = first_4.seed
    where first_4.seed is null

),

final_four as (

    select

        first_4.pick as actual_pick,

        teams.original_pick,
        teams.team
    
    from teams
    inner join first_4 on teams.seed = first_4.seed

),

rest_of_picks as (

    select

        undrawn_teams.pick,

        teams.original_pick,
        teams.team

    from teams
    inner join undrawn_teams on undrawn_teams.seed = teams.original_pick

),

final as (

    select * from final_four
    union
    select * from rest_of_picks

)

select * from final
