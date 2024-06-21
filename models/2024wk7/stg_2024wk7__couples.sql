-- import cte

with source as (

    select * from {{ ref("source_pd2024wk7__couples") }}

),

transformed as (

    select 

    couple,
    to_date(relationship_start, 'MMMM DD, YYYY') as relationship_date,
    case
            when relationship_date < to_date(left(relationship_date, 4) || '-02-14')
            then 'before'
            else 'after'
    end as before_day

    from source

)

select * from transformed