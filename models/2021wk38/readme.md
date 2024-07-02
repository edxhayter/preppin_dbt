# Preppin Data 2021 Week 38:Trilogies

This challenge consisted of two inputs:
<br>

1. \- A data file of films including a trilogy grouping ID\, the film title\, film rating and its sequence in the series\.
2. \- The second file is a list of the top 30 trilogies of all time ranked 1 to 30 with a trilogy name\.

The desired output was a single table with the trilogy ranking, its average rating, the film number within the trilogy, film title, individual film rating and the total number of films in the trilogy.

## Steps

### Staging

The first step was to stage the trilogies file:

```sql

with source as (

    select * from {{ ref('source_2021wk38__trilogies')}}

),

transformed as (

    select

        trilogy_ranking,
        trim(replace(trilogy, 'trilogy', '')) as trilogy

    from source

)

select * from transformed

```

The only actions here were to clean the trilogy field and remove the word 'trilogy' that appeared in every field.

Parsing the films table required a bit of additional work

```sql

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

```

We process the data in two streams using CTEs.

1. The first stream finds the average rating (and max rating to break ties!) grouping by the trilogy grouping field. The grouping field is used later to join back onto the other stream in staging.
2. The second stream is designed to capture the dimensional information splitting the number in series field using split_part to find the sequence number and the total films in the series.

We bring these streams back together using the grouping field and also create our ranking field using Snowflake's window function capability - using a dense rank across the whole table ordering the films by average rating first and then max rating to break ties.

### Final Table

With these staged tables we are ready to make the final table using the ranking we just generated in the last step to bring in the trilogy title.

```sql

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

```

## Skills

A fairly simple challenge but a good opportunity to practice strign functions and a reminder on ranking with window functions in Snowflake. The task to do a 'nested' rank with max_rating serving as a tie-breaker is a good opportunity to learn/remind oneself of the syntax using ',' but also specifying the order for both variables.

```sql
dense_rank() over (order by avg_rating desc, max_rating desc) as trilogy_ranking
```
