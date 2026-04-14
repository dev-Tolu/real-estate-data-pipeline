
  create view "analytics_db"."staging"."stg_listings__dbt_tmp"
    
    
  as (
    WITH raw AS (
    SELECT * FROM "analytics_db"."staging"."stg_raw_listings"
),

-- Deduplicate before cleaning: the scraper paginates 5 pages and the same
-- listing often appears on multiple pages (confirmed in real data). Take the
-- most-recently-extracted copy so we keep the freshest scraped_at timestamp.
deduped AS (
    SELECT DISTINCT ON (property_id)
        *
    FROM raw
    ORDER BY property_id, extracted_at DESC
),

cleaned AS (
    SELECT
        property_id,
        url,
        title,
        location,
        source_site,
        extracted_at::DATE AS listing_date,

        -- Price: strip currency symbol, spaces, commas — leave digits and decimal.
        -- price_raw arrives as text (e.g. '₦850,000,000') from all three sites.
        CAST(
            NULLIF(REGEXP_REPLACE(price_raw, '[^0-9.]', '', 'g'), '')
        AS NUMERIC) AS raw_price_numeric,

        currency_raw,

        -- Beds / Baths / Sqft: Spark now writes these as INTEGER (after ETL fix),
        -- so no REGEXP_REPLACE is needed. The casts below are defensive no-ops that
        -- keep the model safe against legacy dirty rows that predate the ETL fix.
        CAST(NULLIF(bedrooms::TEXT,  '') AS INTEGER) AS bedrooms,
        CAST(NULLIF(bathrooms::TEXT, '') AS INTEGER) AS bathrooms,
        CAST(NULLIF(sqft::TEXT,      '') AS INTEGER) AS sqft,

        -- Standardize Property Type.
        -- Priority: trust the ETL extractor first (URL-derived, reliable),
        -- fall back to title pattern matching for any rows that arrive as 'unknown'.
        CASE
            -- 1. Trust the ETL extractor
            WHEN property_type IS NOT NULL
             AND property_type != 'unknown'        THEN property_type

            -- 2. Apartments / Flats / Rooms
            -- NOTE: patterns are ordered most-specific → least-specific.
            -- '%room%' was the original pattern and is intentionally REMOVED:
            -- it is a superset of '%bedroom%' and misclassified ~86% of listings.
            -- Use only patterns that unambiguously indicate this category.
            WHEN LOWER(title) LIKE '%block of flats%'       THEN 'Block Of Flats'
            WHEN LOWER(title) LIKE '%mini flat%'             THEN 'Mini Flat'
            WHEN LOWER(title) LIKE '%self contain%'          THEN 'Self Contain'
            WHEN LOWER(title) LIKE '%shared apartment%'      THEN 'Shared Apartment'
            WHEN LOWER(title) LIKE '%studio apartment%'      THEN 'Studio Apartment'
            WHEN LOWER(title) LIKE '%penthouse%'             THEN 'Penthouse'
            WHEN LOWER(title) LIKE '%rooms%'
              OR LOWER(title) LIKE '%boys quarter%'          THEN 'Rooms & Boys Quarters'
            WHEN LOWER(title) LIKE '%flat%'
              OR LOWER(title) LIKE '%apartment%'             THEN 'Flats & Apartments'

            -- 3. Houses / Duplexes / Bungalows (specific before generic)
            WHEN LOWER(title) LIKE '%semi-detached duplex%'
              OR LOWER(title) LIKE '%semi detached duplex%'  THEN 'Semi-Detached Duplex'
            WHEN LOWER(title) LIKE '%detached duplex%'
              OR LOWER(title) LIKE '%fully detached duplex%' THEN 'Detached Duplex'
            WHEN LOWER(title) LIKE '%terraced duplex%'
              OR LOWER(title) LIKE '%terrace duplex%'        THEN 'Terraced Duplex'
            WHEN LOWER(title) LIKE '%duplex%'                THEN 'Duplex'

            WHEN LOWER(title) LIKE '%semi-detached bungalow%'
              OR LOWER(title) LIKE '%semi detached bungalow%' THEN 'Semi-Detached Bungalow'
            WHEN LOWER(title) LIKE '%detached bungalow%'
              OR LOWER(title) LIKE '%fully detached bungalow%' THEN 'Detached Bungalow'
            WHEN LOWER(title) LIKE '%terraced bungalow%'    THEN 'Terraced Bungalow'
            WHEN LOWER(title) LIKE '%bungalow%'              THEN 'Bungalow'

            WHEN LOWER(title) LIKE '%townhouse%'             THEN 'Townhouse'
            WHEN LOWER(title) LIKE '%maisonette%'            THEN 'Maisonette'
            WHEN LOWER(title) LIKE '%house%'                 THEN 'House'

            -- 4. Land (specific before generic)
            WHEN LOWER(title) LIKE '%commercial land%'       THEN 'Commercial Land'
            WHEN LOWER(title) LIKE '%industrial land%'       THEN 'Industrial Land'
            WHEN LOWER(title) LIKE '%joint venture land%'    THEN 'Joint Venture Land'
            WHEN LOWER(title) LIKE '%mixed-use land%'
              OR LOWER(title) LIKE '%mixed use land%'        THEN 'Mixed-use Land'
            WHEN LOWER(title) LIKE '%residential land%'      THEN 'Residential Land'
            WHEN LOWER(title) LIKE '%land%'
              OR LOWER(title) LIKE '%plot%'                  THEN 'Land'

            -- 5. Commercial / Other
            WHEN LOWER(title) LIKE '%co-working%'            THEN 'Co-working Space'
            WHEN LOWER(title) LIKE '%conference room%'
              OR LOWER(title) LIKE '%meeting room%'          THEN 'Conference/Meeting Room'
            WHEN LOWER(title) LIKE '%desk%'
              OR LOWER(title) LIKE '%workstation%'           THEN 'Desk/Workstation'
            WHEN LOWER(title) LIKE '%event hall%'            THEN 'Event Hall'
            WHEN LOWER(title) LIKE '%factory%'               THEN 'Factory'
            WHEN LOWER(title) LIKE '%filling station%'
              OR LOWER(title) LIKE '%gas plant%'
              OR LOWER(title) LIKE '%tank farm%'             THEN 'Oil & Gas Property'
            WHEN LOWER(title) LIKE '%hostel%'                THEN 'Hostel'
            WHEN LOWER(title) LIKE '%hotel%'                 THEN 'Hotel'
            WHEN LOWER(title) LIKE '%mall%'
              OR LOWER(title) LIKE '%plaza%'
              OR LOWER(title) LIKE '%complex%'               THEN 'Mall/Complex/Plaza'
            WHEN LOWER(title) LIKE '%office%'                THEN 'Office'
            WHEN LOWER(title) LIKE '%restaurant%'
              OR LOWER(title) LIKE '%bar%'                   THEN 'Restaurant/Bar'
            WHEN LOWER(title) LIKE '%school%'                THEN 'School'
            WHEN LOWER(title) LIKE '%shop%'                  THEN 'Shop'
            WHEN LOWER(title) LIKE '%warehouse%'             THEN 'Warehouse'
            WHEN LOWER(title) LIKE '%commercial%'            THEN 'Commercial Property'

            ELSE 'Other'
        END AS property_type

    FROM deduped
)

SELECT
    property_id,
    url,
    title,
    location,
    location AS address,
    bedrooms,
    bathrooms,
    sqft,
    property_type,
    source_site,
    listing_date,
    currency_raw,
    raw_price_numeric,

    -- NGN conversion for cross-currency comparisons.
    -- TODO: replace the static rate with a join to a ref('fx_rates') table
    -- keyed on (currency, date) — the naira rate moves significantly week to week.
    CASE
        WHEN currency_raw = 'USD' THEN raw_price_numeric * 1500
        ELSE raw_price_numeric
    END AS price_ngn

FROM cleaned
-- Only surface listings where a numeric price was successfully extracted.
-- Rows with NULL price_raw or non-numeric values (e.g. 'Price on Request')
-- are excluded here; track their volume upstream in a data quality check.
WHERE raw_price_numeric IS NOT NULL
  );