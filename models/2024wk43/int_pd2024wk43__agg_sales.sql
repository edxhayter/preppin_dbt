-- challenge asks for aggregation at the product and category level

-- input source data, row level calc of price*quantity done in staging layer

WITH source AS (

    SELECT * FROM {{ ref('stg_pd2024wk43__product_sales') }}

),

-- Calculate the total value sold for each product (in the sub-sample this should be the same grain as the source table but in full data this step will consolidate data for each product into a single row.)

product_sales AS (

    SELECT

        product,
        product_id,
        category,
        price,

        sum(value) AS sales

    FROM source
    GROUP BY 1, 2, 3, 4

),

-- aggrgeate up the data to category level 
category_sales AS (

    SELECT

        category,

        sum(value) AS category_sales

    FROM source
    GROUP BY 1

),

-- join the two levels and compute the % of total within category

pct_total AS (

    SELECT

        product_sales.product,
        product_sales.category,
        product_sales.sales, 
        product_sales.product_id,
        product_sales.price,

        category_sales.category_sales,

        (product_sales.sales/category_sales.category_sales) AS category_pct_total

    FROM product_sales
    INNER JOIN category_sales on product_sales.category = category_sales.category

),

-- stakeholder only wants products responsible for the bottom 15% of sales in each category.
-- we can do this with a qualify filter using a window calc sum partitioned on category and ordering by pct < 15

bottom_15_pct AS (

    SELECT

        category,
        product,
        product_id,
        price,
        sales,
        category_pct_total

    FROM pct_total
    QUALIFY sum(category_pct_total) OVER (PARTITION BY category ORDER BY category_pct_total asc) < 0.15


)

SELECT * FROM bottom_15_pct
ORDER BY category ASC