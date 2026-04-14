{{ config(materialized='table') }}

SELECT DISTINCT
    property_id,
    address,
    location AS city, 
    NULL AS state,
    NULL AS zip_code,
    property_type,
    bedrooms,
    bathrooms,
    sqft AS living_area_sqft,
    source_site,
    title AS listing_title,
    listing_date AS scraped_at
FROM {{ ref('stg_listings') }}