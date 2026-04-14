
  
    

  create  table "analytics_db"."public"."dim_properties__dbt_tmp"
  
  
    as
  
  (
    WITH staged AS (
    SELECT * FROM "analytics_db"."staging"."stg_listings"
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY listing_date DESC) as rn
    FROM staged
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
    source_site
FROM deduplicated
WHERE rn = 1
  );
  