{{ config(materialized='table') }}

SELECT
    property_id,
    price_ngn AS price,
    'NGN' AS currency,
    CASE WHEN currency_raw = 'USD' THEN raw_price_numeric ELSE (price_ngn / 1500) END AS price_usd,
    listing_date,
    CASE WHEN sqft > 0 THEN (price_ngn / sqft) ELSE NULL END AS price_per_sqft,
    'active' AS status,
    listing_date AS created_at
FROM {{ ref('stg_listings') }}