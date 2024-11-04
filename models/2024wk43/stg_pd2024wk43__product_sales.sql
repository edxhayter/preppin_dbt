-- input the seeded challenge data

WITH source AS (

    SELECT * FROM {{ ref('src_pd2024wk43__product_data') }}

),

-- row level calculation for the value of the purchase
-- category field needs trimming of padded spaces

order_value AS (

    SELECT

        * EXCLUDE (category),
        trim(category) as category,
        price*quantity AS value,

    FROM source

)

SELECT * FROM order_value