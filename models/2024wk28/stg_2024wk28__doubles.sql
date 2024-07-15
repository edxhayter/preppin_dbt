with source as (

    select * from {{ ref('source_pd_2024wk28__doubles') }}

),

-- transformed: unpivot, create gender field and remove rows with no winner.
-- also some typos where years have a trailing number so left(year, 4) should fix that

transformed as (

    select 
    
        try_cast(left(year, 4) as integer) as year,
        gender,
        winner
    
    from source
        unpivot (winner for gender in (men, women))

    where 
        year is not null
        and
        winner != 'not held'

),

-- need to split the winner row out into individual rows for each winner with lateral join
final as (

    select

        year,
        gender,
        trim(regexp_replace(value, '"', '')) as winner
        

    from transformed,
        lateral flatten (input => split(winner, ','))

)


select * from final