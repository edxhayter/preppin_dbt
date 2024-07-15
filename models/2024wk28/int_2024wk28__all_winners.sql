-- import CTEs

with singles as (

    select * from {{ ref('stg_2024wk28__singles') }}

),

doubles as (

    select * from {{ ref('stg_2024wk28__doubles') }}

),

mixed as (

   select * from {{ ref('stg_2024wk28__mixed') }} 
    
),

-- Union the outputs into a single data table

unioned as (

select year, gender, winner_name as winner, 'singles' as format from singles
union all
select year, gender, winner, 'doubles' as format from doubles
union all
select year, gender, winner, 'mixed doubles' as format from mixed

)

select * from unioned