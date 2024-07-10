-- import Seed

with source as (

    select * from {{ ref('source_2021wk27__odds') }}

),

-- unpivot the pick columns

unpivot as (

    select 
    
        seed,
        pick,
        (odds*10) as scaled_odds,
        sum(scaled_odds) over (partition by pick order by seed asc) as upper_join_clause,
        
    from source

    unpivot(odds for pick in (pick_1, pick_2, pick_3, pick_4))

),

-- Create the join clauses for the scaffold

transformed as (

    select

        *,
        lag(upper_join_clause, 1, 0) over (partition by pick order by seed asc) as lower_join_clause

    from unpivot

)

select * from transformed


