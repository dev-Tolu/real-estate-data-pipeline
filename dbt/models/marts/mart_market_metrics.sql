WITH daily_agg AS (
    SELECT 
        listing_date,
        -- We'll group by a generic location for now, or you can join dim_properties here
        AVG(price) AS average_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price,
        COUNT(property_id) AS inventory_count
    FROM {{ ref('fct_price_history') }}
    GROUP BY listing_date
)

SELECT 
    listing_date,
    average_price,
    median_price,
    inventory_count,
    -- Spark's Window.partitionBy().orderBy().rowsBetween(-29, 0)
    AVG(average_price) OVER (
        ORDER BY listing_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS rolling_30d_avg_price
FROM daily_agg