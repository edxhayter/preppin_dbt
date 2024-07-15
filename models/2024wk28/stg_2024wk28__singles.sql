with source as (

    select * from {{ ref('source_pd_2024wk28__singles') }}

),

-- transformed: unpivot, create gender field and remove rows with no winner.
-- also some typos where years have a trailing number so left(year, 4) should fix that
-- years with no winners means the winner value does not contain a bracket filter out those instances as well.

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
        contains(winner, '(') = TRUE

),

-- Parse out the nationalities into a seperate column, done in a seperate CTE to first get rid of the rows that need to be filtered out.

final as (

    select

        year,
        gender,
        trim(left(winner, position('(', winner)-1)) as winner_name,
        regexp_substr(winner, '\\((.*)\\)', 1, 1, 'e', 1) as nationality

    from transformed

)

select * from final