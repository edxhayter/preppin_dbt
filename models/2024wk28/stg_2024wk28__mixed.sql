with source as (

    select * from {{ ref('source_pd_2024wk28__mixed') }}

),

-- select just the year and champions columns.
-- the genders are split over new lines
-- male participant is always listed first so gender value appopriately.

transformed as (

    select

        year,
        trim(
            case
                when contains(value, '(')
                then regexp_substr(value, '(.*)\\(', 1, 1, 'e', 1) 
                else value
            end) as winner,
        case
            when ROW_NUMBER() OVER (PARTITION BY year ORDER BY NULL) = 1 then 'MEN'
            else 'WOMEN'
        end as gender

    from source,
        lateral flatten(split(champions, '\n'))

    where winner != 'No competition'

)

select * from transformed