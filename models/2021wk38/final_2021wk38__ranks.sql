-- import CTEs

with ratings as (
    
    select * from {{ ref('stg_2021wk38__films')}}

),

trilogies as (

    select * from {{ ref('stg_2021wk38__trilogies')}}

),

final as(

    select 

        ratings.*,
        trilogies.trilogy

    from ratings
    left join trilogies on trilogies.trilogy_ranking = ratings.trilogy_ranking

)

select * from final