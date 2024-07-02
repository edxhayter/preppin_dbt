with source as (

    select * from {{ ref('source_2021W25__Mega_Evolutions') }}

),

-- remove mega prefix for join and the second word (X/Y), no pokemon have two word names so can split on \s
transformed as (

    select

        name as full_name,
        split_part(trim(replace(name, 'Mega', '')), ' ', 1) as poke_name

    from source

)


select * from transformed