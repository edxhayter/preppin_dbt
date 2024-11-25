-- bring in staged data and date spine

WITH staged AS (

    SELECT * FROM {{ ref('stg_pd2024wk39__engagements') }}

),

date_spine AS (

    SELECT * FROM {{ ref('__2023_2024_date_scaffold') }}

),


-- scaffold out the data for one row per day by joining to the date spine

scaffold AS (

    SELECT

    date_spine.transaction_date AS consulting_date,

    staged.initials,
    staged.engagement_order,
    staged.grade_name,
    staged.day_rate

    FROM staged
    INNER JOIN date_spine ON staged.start_date >= date_spine.transaction_date
    AND staged.end_date <= date_spine.transaction_date

),

-- remove weekend days

suppressed AS (

    SELECT * FROM scaffold

),

aggregated AS (

    SELECT 

        initials,
        engagement_order,
        grade_name,
        day_rate,

        count(distinct consulting_date) as calendar_days

    FROM suppressed
    GROUP BY 1, 2, 3, 4

),

totals AS (

    SELECT 

        *,
        rank() OVER (ORDER BY calendar_days*day_rate desc) AS overall_rank,
        rank() OVER (PARTITION BY grade_name ORDER BY calendar_days*day_rate desc) as grade_rank

    FROM aggregated



)

SELECT * FROM totals
ORDER BY overall_rank desc