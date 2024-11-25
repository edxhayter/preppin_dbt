WITH source AS (

    SELECT * FROM {{ ref('src_pd2024wk39__engagements') }}

),

-- ensure correct data types

transformed AS (

    SELECT
    
        to_date(engagement_start_date, 'DD/MM/YYYY') AS start_date,
        to_date(engagement_end_date, 'DD/MM/YYYY') AS end_date,
        trim(initials) AS initials,
        engagement_order,
        trim(grade_name) AS grade_name,
        day_rate

    FROM source
)

SELECT * FROM transformed