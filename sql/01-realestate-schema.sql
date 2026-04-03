-- ============================================
-- REAL ESTATE DATA PIPELINE - MAIN SCHEMA
-- ============================================

-- Enable extensions
-- NOTE: postgis and timescaledb require a custom image (e.g. timescale/timescaledb-ha or postgis/postgis).
-- If using the default postgres:15-alpine image, remove or comment out the extension lines below
-- and remove the create_hypertable() calls.
-- CREATE EXTENSION IF NOT EXISTS postgis;
-- CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================
-- CORE TABLES
-- ============================================

-- Properties table
CREATE TABLE IF NOT EXISTS properties (
    id SERIAL PRIMARY KEY,
    property_id VARCHAR(100) UNIQUE NOT NULL,
    address TEXT NOT NULL,
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    property_type VARCHAR(50),
    year_built INTEGER,
    lot_size_sqft DECIMAL(12, 2),
    living_area_sqft DECIMAL(10, 2),
    bedrooms INTEGER,
    bathrooms DECIMAL(4, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Price history table
CREATE TABLE IF NOT EXISTS price_history (
    id SERIAL PRIMARY KEY,
    property_id VARCHAR(100) REFERENCES properties(property_id),
    price DECIMAL(12, 2) NOT NULL,
    listing_date DATE NOT NULL,
    sale_date DATE,
    price_per_sqft DECIMAL(10, 2),
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(property_id, listing_date)
);

SELECT create_hypertable('price_history', 'listing_date', if_not_exists => TRUE);

-- Market metrics table
CREATE TABLE IF NOT EXISTS market_metrics (
    id SERIAL PRIMARY KEY,
    metric_date DATE NOT NULL,
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    median_price DECIMAL(12, 2),
    average_price DECIMAL(12, 2),
    median_price_per_sqft DECIMAL(10, 2),
    inventory_count INTEGER,
    days_on_market_avg INTEGER,
    sales_volume INTEGER,
    price_mom_pct DECIMAL(5, 2),
    price_yoy_pct DECIMAL(5, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(metric_date, zip_code)
);

SELECT create_hypertable('market_metrics', 'metric_date', if_not_exists => TRUE);

-- Forecast results table
CREATE TABLE IF NOT EXISTS price_forecasts (
    id SERIAL PRIMARY KEY,
    property_id VARCHAR(100) REFERENCES properties(property_id),
    forecast_date DATE NOT NULL,
    predicted_price DECIMAL(12, 2),
    confidence_lower DECIMAL(12, 2),
    confidence_upper DECIMAL(12, 2),
    model_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Scraper job logs
CREATE TABLE IF NOT EXISTS scraper_logs (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(100),
    source_url TEXT,
    records_scraped INTEGER,
    status VARCHAR(50),
    error_message TEXT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    duration_seconds INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ETL job tracking
CREATE TABLE IF NOT EXISTS etl_jobs (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(100),
    run_id VARCHAR(100) UNIQUE,
    status VARCHAR(50),
    records_processed INTEGER,
    error_message TEXT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    duration_seconds INTEGER,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_properties_location ON properties(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_properties_zip ON properties(zip_code);
CREATE INDEX IF NOT EXISTS idx_price_history_property ON price_history(property_id, listing_date);
CREATE INDEX IF NOT EXISTS idx_market_metrics_date ON market_metrics(metric_date DESC);
CREATE INDEX IF NOT EXISTS idx_forecasts_property ON price_forecasts(property_id, forecast_date DESC);

-- Create view for current prices
CREATE OR REPLACE VIEW property_current_prices AS
SELECT DISTINCT ON (p.property_id)
    p.property_id,
    p.address,
    p.city,
    p.state,
    p.zip_code,
    ph.price as current_price,
    ph.listing_date as last_listing_date
FROM properties p
LEFT JOIN price_history ph ON p.property_id = ph.property_id
ORDER BY p.property_id, ph.listing_date DESC;

-- Materialized view for market trends
CREATE MATERIALIZED VIEW IF NOT EXISTS market_trends_30d AS
SELECT 
    zip_code,
    metric_date,
    median_price,
    average_price,
    inventory_count,
    days_on_market_avg
FROM market_metrics
ORDER BY zip_code, metric_date DESC;

-- Update timestamp function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_properties_updated_at
    BEFORE UPDATE ON properties
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Create read-only user for Grafana
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_user WHERE usename = 'grafana_reader') THEN
        CREATE USER grafana_reader WITH PASSWORD 'GrafanaReadOnly2024!';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE realestate_db TO grafana_reader;
GRANT USAGE ON SCHEMA public TO grafana_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_reader;


-- ============================================
-- SCHEMA UPDATES FOR NIGERIAN PROPERTY DATA
-- ============================================

-- 1. ADD CURRENCY SUPPORT TO PRICE_HISTORY
-- Some listings (PrivateProperty) show prices in USD
ALTER TABLE price_history 
ADD COLUMN IF NOT EXISTS currency VARCHAR(3) DEFAULT 'NGN',
ADD COLUMN IF NOT EXISTS price_usd DECIMAL(15, 2);

-- 2. ADD AGENT/SELLER INFORMATION
-- Many listings show the agent/company name
ALTER TABLE properties 
ADD COLUMN IF NOT EXISTS agent_name VARCHAR(255),
ADD COLUMN IF NOT EXISTS agent_phone VARCHAR(50),
ADD COLUMN IF NOT EXISTS agent_email VARCHAR(255),
ADD COLUMN IF NOT EXISTS listing_title TEXT,  -- Original listing title
ADD COLUMN IF NOT EXISTS scraped_at TIMESTAMP,  -- When this record was scraped
ADD COLUMN IF NOT EXISTS source_site VARCHAR(100);  -- Which site it came from

-- 3. ADD PRICE RANGE CATEGORIES FOR BETTER FILTERING
-- Useful for Nigerian market with wide price ranges
ALTER TABLE properties 
ADD COLUMN IF NOT EXISTS price_range VARCHAR(50);  -- e.g., 'Budget', 'Mid-Range', 'Luxury'

-- 4. ADD PROPERTY FEATURES
-- Store amenities and features from listings
CREATE TABLE IF NOT EXISTS property_features (
    id SERIAL PRIMARY KEY,
    property_id VARCHAR(255) REFERENCES properties(property_id),
    feature_name VARCHAR(100),
    feature_value VARCHAR(255),  -- For features with values (e.g., 'Parking Spaces: 2')
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(property_id, feature_name)
);

-- Common features to track
-- - swimming_pool, gym, elevator, security, parking_spaces, furnished, serviced, etc.

-- 5. ADD LISTING STATUS TRACKING
ALTER TABLE price_history 
ADD COLUMN IF NOT EXISTS listing_status VARCHAR(50),  -- 'active', 'sold', 'rented', 'expired'
ADD COLUMN IF NOT EXISTS days_on_market INTEGER;

-- 6. ADD SCRAPER METADATA TABLE
-- Track which fields were successfully scraped
CREATE TABLE IF NOT EXISTS scraper_quality_metrics (
    id SERIAL PRIMARY KEY,
    source_url TEXT,
    job_id VARCHAR(100),
    total_listings INTEGER,
    listings_with_price INTEGER,
    listings_with_location INTEGER,
    listings_with_bedrooms INTEGER,
    listings_with_images INTEGER,
    quality_score DECIMAL(5, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 7. ADD CITY/STATE MAPPING for Nigerian locations
CREATE TABLE IF NOT EXISTS nigeria_locations (
    id SERIAL PRIMARY KEY,
    state_name VARCHAR(100),
    city_name VARCHAR(100),
    lga_name VARCHAR(100),  -- Local Government Area
    zip_code VARCHAR(20),
    region VARCHAR(50),  -- North, South, East, West
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert common Nigerian states
INSERT INTO nigeria_locations (state_name, region) VALUES
('Lagos', 'South-West'),
('Abuja', 'North-Central'),
('Rivers', 'South-South'),
('Ogun', 'South-West'),
('Oyo', 'South-West'),
('Anambra', 'South-East'),
('Enugu', 'South-East'),
('Kano', 'North-West'),
('Kaduna', 'North-West'),
('Delta', 'South-South'),
('Edo', 'South-South'),
('Imo', 'South-East'),
('Abia', 'South-East'),
('Akwa Ibom', 'South-South'),
('Cross River', 'South-South'),
('Plateau', 'North-Central')
ON CONFLICT DO NOTHING;

-- 8. ADD INDEXES FOR NEW COLUMNS
CREATE INDEX IF NOT EXISTS idx_properties_agent ON properties(agent_name);
CREATE INDEX IF NOT EXISTS idx_properties_source_site ON properties(source_site);
CREATE INDEX IF NOT EXISTS idx_price_history_currency ON price_history(currency);
CREATE INDEX IF NOT EXISTS idx_properties_price_range ON properties(price_range);

-- 9. UPDATE MATERIALIZED VIEW to include currency
DROP MATERIALIZED VIEW IF EXISTS market_trends_30d;
CREATE MATERIALIZED VIEW market_trends_30d AS
SELECT
    p.state,
    p.city,
    ph.currency,
    mm.metric_date,
    mm.median_price,
    mm.average_price,
    mm.inventory_count,
    mm.days_on_market_avg
FROM market_metrics mm
LEFT JOIN properties p ON p.zip_code = mm.zip_code
WHERE mm.metric_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY p.state, p.city, mm.metric_date DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_market_trends_30d_state_city_date 
    ON market_trends_30d(state, city, metric_date DESC);


-- Store features as JSONB for flexibility
ALTER TABLE properties 
ADD COLUMN IF NOT EXISTS features JSONB,
ADD COLUMN IF NOT EXISTS description TEXT;

-- Or create a normalized features table (better for querying)
CREATE TABLE IF NOT EXISTS property_amenities (
    id SERIAL PRIMARY KEY,
    property_id VARCHAR(255) REFERENCES properties(property_id),
    amenity_type VARCHAR(100),
    amenity_value VARCHAR(255),  -- For amenities with values (e.g., parking_spaces: 2)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(property_id, amenity_type)
);

-- Common amenities to track (all available from all three sites)
INSERT INTO amenity_types (amenity_name) VALUES
('swimming_pool'), ('gym'), ('elevator'), ('security'), ('parking_spaces'),
('furnished'), ('serviced'), ('bq'), ('backup_power'), ('water_supply'),
('air_conditioning'), ('balcony'), ('jacuzzi'), ('walk_in_closet'),
('cinema'), ('roof_terrace'), ('playground'), ('restaurant'), ('barbecue')
ON CONFLICT DO NOTHING;

