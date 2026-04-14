-- ============================================
-- REAL ESTATE DATA PIPELINE - STAGING SCHEMA
-- ============================================
-- Create the staging area
CREATE SCHEMA IF NOT EXISTS staging;

-- Spark will write to this table.
-- FIX: bedrooms, bathrooms, and sqft are now INTEGER (not TEXT).
-- The ETL (etl.py) always returns int from _parse_int() and declares these
-- as IntegerType in listing_schema. The old TEXT columns caused Spark's JDBC
-- writer to either coerce silently or error, depending on the driver version.
-- If you have an existing table, run:
--   ALTER TABLE staging.stg_raw_listings
--       ALTER COLUMN bedrooms  TYPE INTEGER USING bedrooms::integer,
--       ALTER COLUMN bathrooms TYPE INTEGER USING bathrooms::integer,
--       ALTER COLUMN sqft      TYPE INTEGER USING sqft::integer;
CREATE TABLE IF NOT EXISTS staging.stg_raw_listings (
    property_id         VARCHAR(100),
    url                 TEXT,
    title               TEXT,
    price_raw           TEXT,
    currency_raw        VARCHAR(10),
    location            TEXT,
    bedrooms            INTEGER,
    bathrooms           INTEGER,
    sqft                INTEGER,
    property_type       TEXT,
    source_site         VARCHAR(100),
    extracted_at        TIMESTAMP,
    spark_processed_at  TIMESTAMP
);


-- ============================================
-- REAL ESTATE DATA PIPELINE - MAIN SCHEMA
-- ============================================

-- Enable extensions
-- NOTE: postgis and timescaledb require a custom image.
-- If using the default postgres:15-alpine image, remove these lines.
-- CREATE EXTENSION IF NOT EXISTS postgis;
-- CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================
-- CORE TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS properties (
    id                SERIAL PRIMARY KEY,
    property_id       VARCHAR(100) UNIQUE NOT NULL,
    address           TEXT,
    city              VARCHAR(100),
    state             VARCHAR(50),
    zip_code          VARCHAR(20),
    latitude          DECIMAL(10, 8),
    longitude         DECIMAL(11, 8),
    property_type     VARCHAR(255),
    year_built        INTEGER,
    lot_size_sqft     DECIMAL(12, 2),
    living_area_sqft  DECIMAL(10, 2),
    bedrooms          INTEGER,
    bathrooms         DECIMAL(4, 2),
    agent_name        VARCHAR(255),
    agent_phone       VARCHAR(50),
    agent_email       VARCHAR(255),
    listing_title     TEXT,
    scraped_at        TIMESTAMP,
    source_site       VARCHAR(100),
    price_range       VARCHAR(50),
    features          JSONB,
    description       TEXT,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS price_history (
    id               SERIAL PRIMARY KEY,
    property_id      VARCHAR(100) REFERENCES properties(property_id),
    price            DECIMAL(15, 2) NOT NULL,
    currency         VARCHAR(3) DEFAULT 'NGN',
    price_usd        DECIMAL(15, 2),
    listing_date     DATE NOT NULL,
    sale_date        DATE,
    price_per_sqft   DECIMAL(10, 2),
    status           VARCHAR(50),
    listing_status   VARCHAR(50),
    days_on_market   INTEGER,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (property_id, listing_date)
);

CREATE TABLE IF NOT EXISTS market_metrics (
    id                     SERIAL PRIMARY KEY,
    metric_date            DATE NOT NULL,
    city                   VARCHAR(100),
    state                  VARCHAR(50),
    zip_code               VARCHAR(20),
    median_price           DECIMAL(15, 2),
    average_price          DECIMAL(15, 2),
    median_price_per_sqft  DECIMAL(10, 2),
    inventory_count        INTEGER,
    days_on_market_avg     INTEGER,
    sales_volume           INTEGER,
    price_mom_pct          DECIMAL(5, 2),
    price_yoy_pct          DECIMAL(5, 2),
    created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (metric_date, zip_code)
);

CREATE TABLE IF NOT EXISTS price_forecasts (
    id                SERIAL PRIMARY KEY,
    property_id       VARCHAR(100) REFERENCES properties(property_id),
    forecast_date     DATE NOT NULL,
    predicted_price   DECIMAL(15, 2),
    confidence_lower  DECIMAL(15, 2),
    confidence_upper  DECIMAL(15, 2),
    model_version     VARCHAR(50),
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (property_id, forecast_date)
);

CREATE TABLE IF NOT EXISTS scraper_logs (
    id                SERIAL PRIMARY KEY,
    job_id            VARCHAR(100),
    source_url        TEXT,
    records_scraped   INTEGER,
    status            VARCHAR(50),
    error_message     TEXT,
    started_at        TIMESTAMP,
    completed_at      TIMESTAMP,
    duration_seconds  INTEGER,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_jobs (
    id                 SERIAL PRIMARY KEY,
    job_name           VARCHAR(100),
    run_id             VARCHAR(100) UNIQUE,
    status             VARCHAR(50),
    records_processed  INTEGER,
    error_message      TEXT,
    started_at         TIMESTAMP,
    completed_at       TIMESTAMP,
    duration_seconds   INTEGER,
    metadata           JSONB,
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS property_features (
    id             SERIAL PRIMARY KEY,
    property_id    VARCHAR(255) REFERENCES properties(property_id),
    feature_name   VARCHAR(100),
    feature_value  VARCHAR(255),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (property_id, feature_name)
);

CREATE TABLE IF NOT EXISTS property_amenities (
    id              SERIAL PRIMARY KEY,
    property_id     VARCHAR(255) REFERENCES properties(property_id),
    amenity_type    VARCHAR(100),
    amenity_value   VARCHAR(255),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (property_id, amenity_type)
);

CREATE TABLE IF NOT EXISTS scraper_quality_metrics (
    id                        SERIAL PRIMARY KEY,
    source_url                TEXT,
    job_id                    VARCHAR(100),
    total_listings            INTEGER,
    listings_with_price       INTEGER,
    listings_with_location    INTEGER,
    listings_with_bedrooms    INTEGER,
    listings_with_images      INTEGER,
    quality_score             DECIMAL(5, 2),
    created_at                TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS nigeria_locations (
    id          SERIAL PRIMARY KEY,
    state_name  VARCHAR(100),
    city_name   VARCHAR(100),
    lga_name    VARCHAR(100),
    zip_code    VARCHAR(20),
    region      VARCHAR(50),
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO nigeria_locations (state_name, region) VALUES
    ('Lagos',       'South-West'),
    ('Abuja',       'North-Central'),
    ('Rivers',      'South-South'),
    ('Ogun',        'South-West'),
    ('Oyo',         'South-West'),
    ('Anambra',     'South-East'),
    ('Enugu',       'South-East'),
    ('Kano',        'North-West'),
    ('Kaduna',      'North-West'),
    ('Delta',       'South-South'),
    ('Edo',         'South-South'),
    ('Imo',         'South-East'),
    ('Abia',        'South-East'),
    ('Akwa Ibom',   'South-South'),
    ('Cross River', 'South-South'),
    ('Plateau',     'North-Central')
ON CONFLICT DO NOTHING;

-- ============================================
-- INDEXES
-- ============================================

CREATE INDEX IF NOT EXISTS idx_properties_location     ON properties(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_properties_zip          ON properties(zip_code);
CREATE INDEX IF NOT EXISTS idx_properties_agent        ON properties(agent_name);
CREATE INDEX IF NOT EXISTS idx_properties_source_site  ON properties(source_site);
CREATE INDEX IF NOT EXISTS idx_properties_price_range  ON properties(price_range);
CREATE INDEX IF NOT EXISTS idx_price_history_property  ON price_history(property_id, listing_date);
CREATE INDEX IF NOT EXISTS idx_price_history_currency  ON price_history(currency);
CREATE INDEX IF NOT EXISTS idx_market_metrics_date     ON market_metrics(metric_date DESC);
CREATE INDEX IF NOT EXISTS idx_forecasts_property      ON price_forecasts(property_id, forecast_date DESC);

-- ============================================
-- VIEWS
-- ============================================

CREATE OR REPLACE VIEW property_current_prices AS
SELECT DISTINCT ON (p.property_id)
    p.property_id,
    p.address,
    p.city,
    p.state,
    p.zip_code,
    ph.price            AS current_price,
    ph.listing_date     AS last_listing_date
FROM properties p
LEFT JOIN price_history ph ON p.property_id = ph.property_id
ORDER BY p.property_id, ph.listing_date DESC;

CREATE MATERIALIZED VIEW IF NOT EXISTS market_trends_30d AS
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
LEFT JOIN properties p  ON p.zip_code = mm.zip_code
LEFT JOIN price_history ph ON ph.property_id = (
    SELECT property_id FROM properties WHERE zip_code = mm.zip_code LIMIT 1
)
WHERE mm.metric_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY p.state, p.city, mm.metric_date DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_market_trends_30d_state_city_date
    ON market_trends_30d(COALESCE(state, ''), COALESCE(city, ''), metric_date DESC);

-- ============================================
-- TRIGGERS
-- ============================================

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_properties_updated_at ON properties;
CREATE TRIGGER update_properties_updated_at
    BEFORE UPDATE ON properties
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================
-- GRAFANA READ-ONLY USER
-- ============================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_user WHERE usename = 'grafana_reader') THEN
        CREATE USER grafana_reader WITH PASSWORD 'GrafanaReadOnly2026';
    END IF;
END
$$;
ALTER USER grafana_reader WITH PASSWORD 'GrafanaReadOnly2026';

GRANT CONNECT ON DATABASE analytics_db TO grafana_reader;
GRANT USAGE ON SCHEMA public TO grafana_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO grafana_reader;