WITH raw AS (
    SELECT * FROM {{ source('staging', 'stg_raw_listings') }}
),

cleaned AS (
    SELECT
        property_id,
        url,
        title,
        location,
        source_site,
        extracted_at::DATE AS listing_date,
        
        -- Clean Price: Strip everything except numbers and decimals
        CAST(NULLIF(REGEXP_REPLACE(price_raw, '[^0-9.]', '', 'g'), '') AS NUMERIC) AS raw_price_numeric,
        currency_raw,
        
        -- Clean Beds/Baths/Sqft: Strip non-numeric characters and cast to integer
        CAST(NULLIF(REGEXP_REPLACE(bedrooms, '[^0-9]', '', 'g'), '') AS INTEGER) AS bedrooms,
        CAST(NULLIF(REGEXP_REPLACE(bathrooms, '[^0-9]', '', 'g'), '') AS INTEGER) AS bathrooms,
        CAST(NULLIF(REGEXP_REPLACE(sqft, '[^0-9]', '', 'g'), '') AS INTEGER) AS sqft,

        -- Standardize Property Type using SQL pattern matching on the title
        CASE 
            WHEN LOWER(title) LIKE '%flat%' OR LOWER(title) LIKE '%apartment%' THEN 'flat/apartment'
            WHEN LOWER(title) LIKE '%detached%' OR LOWER(title) LIKE '%duplex%' OR LOWER(title) LIKE '%terrace%' OR LOWER(title) LIKE '%bungalow%' THEN 'house'
            WHEN LOWER(title) LIKE '%land%' OR LOWER(title) LIKE '%plot%' THEN 'land'
            WHEN LOWER(title) LIKE '%commercial%' OR LOWER(title) LIKE '%office%' THEN 'commercial'
            ELSE COALESCE(NULLIF(LOWER(property_type), 'unknown'), 'unknown')
        END AS property_type

    FROM raw
)

SELECT 
    property_id,
    url,
    title,
    location,
    bedrooms,
    bathrooms,
    sqft,
    property_type,
    source_site,
    listing_date,
    
    -- Handle Currency Conversion (PrivateProperty USD listings)
    -- Using a static 1500 rate. In a production system, this would join to an FX rate table.
    CASE 
        WHEN currency_raw = 'USD' THEN raw_price_numeric * 1500
        ELSE raw_price_numeric
    END AS price_ngn

FROM cleaned
-- Filter out rows where the scraper found a card but no actual price
WHERE raw_price_numeric IS NOT NULL