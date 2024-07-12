# Preppin Data 2021 Week 27

### Context

This weeks challenge required us to make a generator for the first 4 picks of the NBA Draft that are done by lottery in real life. For the other picks in the lottery, the challenge breaks from reality and asks us to assign the picks in seed order from the remaining teams.

The inputs we recieved included a 1000 row scaffold table, a list of teams and their seed and finally an odds matrix for every seeds chances at being given the particular pick.

### Important Models

For the purpose of simulating the draw and making a pool of picks to sample from, I opted to unpivot the odds table, multiply the odds by 10 to remove the decimals so that the odds equated to a row in a table that represented the pool of potential picks. The idea from here was to assign random values, sort by random value and sample the top row.

###### Unpivot

<br>
``` sql
-- import Seed

with source as (

select \* from {{ ref('source_2021wk27__odds') }}

),

-- unpivot the pick columns

unpivot as (

select

seed,
        pick,
        (odds\*10) as scaled_odds,
        sum(scaled_odds) over (partition by pick order by seed asc) as upper_join_clause,

from source

unpivot(odds for pick in (pick_1, pick_2, pick_3, pick_4))

),

-- Create the join clauses for the scaffold

transformed as (

select

\*,
        lag(upper_join_clause, 1, 0) over (partition by pick order by seed asc) as lower_join_clause

from unpivot

)

select \* from transformed

````

I believe the lower-bound clause was unecessary but I opted to use it for two reasons; to refresh my use of the lag function and hopefully more clearly communicate to a new developer the join transformation I was aiming to achieve.

###### Join Scaffold

``` sql
-- import CTEs

with scaffold as (

    select * from {{ ref('base_2021wk27__scaffold') }}

),

odds as (

    select * from {{ ref('stg_2021wk27__odds') }}

),

-- join odds and scaffold using the less than and greater than join terms then the table is ready to sample from iteratively

odds_sample as (

    select

        odds.seed,
        odds.pick,
        random() as sort,
        scaffold.scaffold

    from odds
    inner join scaffold on scaffold.scaffold > odds.lower_join_clause and scaffold.scaffold <= odds.upper_join_clause
    order by cast(right(pick,1) as integer) asc, seed asc, scaffold asc


)

select * from odds_sample
````

###### Simulating the Picks

The hardest part of the challenge was simulating the picks. In a simple form I considered making a CTE for each pick with where clauses to remove rows that were for the wrong pick or were a team that had been picked earlier.

However, I thought rather than writing that long query I wondered if I could leverage JINJA in dbt to streamline the script. The end result was not perfect rather than develop in parallel (in the sense of having the original approach and then a JINJA alternative), I had a mix where I wrote the JINJA for loop for picks 2-4 but left pick 1 as a static part of the query (so did not add pick 1 into the loop logic) leaving a bit of a mixed approach. But I have left it as so in order to learn from the experience and move on.

```sql
-- import CTEs

with pool as (

    select * from {{ ref('int_2021wk27__odds_pool') }}

),

-- logic for making the first pick

pick_1 as (

    select *
    from pool
    where pick = 'PICK_1'
    order by sort desc
    limit 1

)

{% set ctes = ["pick_2", "pick_3", "pick_4"] %}

{% for cte in ctes %}

, {{ cte }} as (

    select *
    from pool
    where pick = '{{ cte | upper }}'
    and seed not in (select seed from pick_1)
    {% if loop.index > 0 %}
    {% for prev_cte in ctes[:loop.index0] %}
        and seed not in (select seed from {{ prev_cte }})
    {% endfor %}
    {% endif %}
    order by sort desc
    limit 1

)

{% endfor %},

combined as (

    select * from pick_1
    union
    select * from pick_2
    union
    select * from pick_3
    union
    select * from pick_4

),

final as (

    select

        seed,
        cast(right(pick, 1) as integer) as pick

    from combined

)

select * from final
```

The for loop creates a CTE for each of the values in the vector ctes. Selecting all fields where the pick == the uppercase CTE and the seed is not the seed selected in pick 1 and if the loop is running for the second time or later (index being 0 based here) then for all the previous runs make sure the seed is not in that CTE.

Each CTE from this loop should be a singular row with a singular team, so the last step is to union each of these rows. The final stage of adding unpicked teams as the rest of the picks in order without odds is fairly straightforward and has been excluded.
