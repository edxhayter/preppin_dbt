-- import CTEs

with winners as (

    select * from {{ ref('int_2024wk28__all_winners') }}

),

transformations as (

    select

        year,
        gender,
        winner as champion,
        case
            when format = 'mixed doubles'
            then initcap(format)
            else initcap(gender) || '`s ' || initcap(format)
        end as comp
    
    from winners


),

recent_win as (

    select

        champion,
        max(year) as most_recent_win,

    from transformations
    group by champion

),

tournament_wins as (

    select 

        *

    from transformations
        pivot(count(year) for comp in (
            'Men`s Singles',
            'Men`s Doubles',
            'Women`s Singles',
            'Women`s Doubles',
            'Mixed Doubles'
        ))

),

-- to-do: rename the fields in a database friendly format
-- filter on whether mens singles > 0 and mens doubles > 0 or mixed doubles > 0 or womens equivilent

winner_subset as (

    select

        champion,
        gender,
        "'Women`s Singles'" as womens_singles,
        "'Women`s Doubles'" as womens_doubles,
        "'Men`s Singles'" as mens_singles,
        "'Men`s Doubles'" as mens_doubles,
        "'Mixed Doubles'" as mixed_doubles,
        womens_singles + womens_doubles + mens_singles + mens_doubles + mixed_doubles as total_chips

    from tournament_wins

    where 
        case
            when gender = 'MEN'
            then mens_singles > 0 and (mens_doubles > 0 or mixed_doubles > 0)
        else womens_singles > 0 and (womens_doubles > 0 or mixed_doubles > 0)
        end
),

-- join the filtered view to the most recent win CTE on champion name
-- Compute the total championshipps and rank

final as (

    select

        rank() over (order by winner_subset.total_chips desc) as rank,
        winner_subset.*,
        recent_win.most_recent_win

    from winner_subset
    inner join recent_win on winner_subset.champion = recent_win.champion


)


select * from final