
  
    

  create  table "analytics_db"."public"."fct_price_history__dbt_tmp"
  
  
    as
  
  (
    WITH staged AS (
    SELECT * FROM "analytics_db"."staging"."stg_listings" WHERE price_ngn > 0
),

price_stats AS (
    SELECT 
        AVG(price_ngn) as avg_price, 
        STDDEV(price_ngn) as std_price 
    FROM staged
),

cleaned_prices AS (
    SELECT s.* FROM staged s
    CROSS JOIN price_stats ps
    WHERE s.price_ngn < (ps.avg_price + (3 * ps.std_price))
)

SELECT 
    property_id,
    listing_date,
    price_ngn AS price,
    'NGN' AS currency,
    CASE 
        WHEN price_ngn < 50000000 THEN 'Budget'
        WHEN price_ngn >= 50000000 AND price_ngn < 150000000 THEN 'Mid-Range'
        WHEN price_ngn >= 150000000 AND price_ngn < 500000000 THEN 'High-End'
        ELSE 'Luxury'
    END AS price_range
FROM cleaned_prices
  );
  