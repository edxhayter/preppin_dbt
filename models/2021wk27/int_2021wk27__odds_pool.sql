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