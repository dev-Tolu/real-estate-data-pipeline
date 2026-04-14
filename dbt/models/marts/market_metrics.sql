{{ config(materialized='table') }}

WITH daily_agg AS (
    SELECT 
        listing_date,
        -- Grouping by listing_date as requested
        AVG(price) AS average_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price,
        COUNT(property_id) AS inventory_count
    FROM {{ ref('price_history') }} 
    GROUP BY listing_date
)

SELECT 
    listing_date,
    average_price,
    median_price,
    inventory_count,
    -- Replicating the Spark rolling 30-day average
    AVG(average_price) OVER (
        ORDER BY listing_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS rolling_30d_avg_price
FROM daily_agg