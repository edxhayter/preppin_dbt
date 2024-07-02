with source as (

    select * from {{ ref ("source_2021W25__Gigantamax") }}

),

transformed as (

    select

        name as full_name,
        trim(replace(name, 'Gigantamax', '')) as poke_name

    from source

),

-- flapple and appletun are in the same row - split on '/' and then use a lateral join

split_rows as (

    select

        full_name,
        split_part(value, '/', 1) as poke_name

    from transformed,
    lateral flatten(split(poke_name, '/'))

)

select * from split_rows
