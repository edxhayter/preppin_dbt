{{ config(materialized='table') }}

WITH source AS (

    SELECT * FROM {{ ref('source_pd_2024wk42__scores') }}

),

week_and_stage AS (

    SELECT

        series,

        -- raw fields for next CTE
        week AS raw_week,
        couple,
        scores,
        dance,
        music,
        result,
        film,
        broadway_musical,
        musical,
        country,
        celebratingbbc,

        -- Assign a year based on series number (series 1 and 2 both in 2004 then annually subsequently)
        CASE
            WHEN series = 1 OR series = 2 THEN 2004
            ELSE 2004 + (series - 2)
        END AS year,

        -- REGEX numbers only to get the week number
        regexp_substr(week, '.*(\\d+).*', 1, 1, 'e', 1) as parsed_week,

        -- Take the second part of the week that includes stage and any themes and make separate columns for stage and theme.
        -- Stage with a regex extract for particular phrases
        CASE
            WHEN week LIKE '%Final%' THEN 'Final'
            WHEN week LIKE '%Semifinal%' OR week LIKE'%Semi-final%' THEN 'Semi Final'
            WHEN week LIKE '%Quarterfinal%' OR week LIKE'%Quarter-final%' THEN 'Quarter Final'
        END as stage,

    FROM source
    WHERE couple != 'Couple' 
        -- debug couples comment out otherwise
        -- AND couple = 'Scott & Natalie' OR couple = 'Ashley & Pasha'

),

theme_and_scores AS (

    SELECT

        year,
        series,
        parsed_week as week,
        stage,

        -- process the theme using the raw_week and stage fields
        CASE
            WHEN stage IS NULL THEN iff(split_part(raw_week, ':', 2) = '', NULL, split_part(raw_week, ':', 2))
            ELSE regexp_substr(split_part(raw_week, ':', 2), '(.*?)\\(', 1, 1, 'e', 1)
        END AS theme,

        -- consolidate the theme values into a single theme detail field, coalesce takes the first non-null value and it can only be one of these fields any given week
        coalesce(film, broadway_musical,musical, country,celebratingbbc) AS theme_detail,

        -- split scores up from judges scores and overall score
        CASE
            WHEN contains(scores, '(')
                THEN left( scores, position( '(' IN scores )-1 )
            ELSE scores 
        END AS overall_score,
        regexp_substr(scores, '\\((.*)\\)', 1, 1, 'e', 1) AS judges_scores,

        -- remaining fields to process (these involve changes to row count so have been left to an intermediate layer)
        couple,
        dance,
        music,
        result
    
    from week_and_stage

)

select * from theme_and_scores