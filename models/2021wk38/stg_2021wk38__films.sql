with source as (

    select * from {{ ref('source_2021wk38__films')}}

),

ratings as (

    select

        trilogy_grouping as grouping,
        avg(rating) as avg_rating,
        max(rating) as max_rating

    from source
    group by grouping

),

series_info as (

    select

        title,
        split_part(number_in_series, '/', 1) as film_order,
        split_part(number_in_series, '/', 2) as total_films_in_series,
        trilogy_grouping as grouping

    from source

),

combined as (

    select

        series_info.*,

        ratings.avg_rating,
        ratings.max_rating,
        dense_rank() over (order by avg_rating desc, max_rating desc) as trilogy_ranking

    from series_info
    join ratings on series_info.grouping = ratings.grouping

)

select * from combined