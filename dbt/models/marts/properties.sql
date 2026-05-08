
{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} ADD CONSTRAINT properties_property_id_unique UNIQUE (property_id)"
) }}



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
    listing_date AS scraped_at,
    current_timestamp AS created_at
FROM {{ ref('stg_listings') }}
