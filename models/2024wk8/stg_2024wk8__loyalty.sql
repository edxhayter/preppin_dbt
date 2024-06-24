with source as (

    select * from {{ ref("source_pd2024wk8__loyalty" ) }}

),

transformed as (

    select

        tier_grouping as bin_size,
        number_of_flights,
        replace(Tier, 'Tier', '') as tier,
        benefits,

    from source
)

select * from transformed