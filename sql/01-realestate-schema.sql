-- ============================================
-- REAL ESTATE DATA PIPELINE - MAIN SCHEMA
-- ============================================
-- Compatible with: postgres:17-alpine (plain PostgreSQL, no extensions)
-- Removed: TimescaleDB create_hypertable() calls — requires a separate
--          timescaledb image and is overkill for this pipeline's data volume.
--          Plain PostgreSQL with good indexes gives equivalent performance
--          at the scale of Nigerian property listings.
-- Removed: PostGIS extension — lat/lng stored as plain DECIMAL is sufficient
--          unless you need spatial queries (e.g. ST_Within, radius searches).
--          Re-enable by switching to postgis/postgis:17-alpine image.
-- ============================================

-- ============================================
-- CORE TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS properties (
    id                  SERIAL PRIMARY KEY,
    property_id         VARCHAR(255) UNIQUE NOT NULL,  -- increased from 100 to match scraper
    address             TEXT,                           -- relaxed NOT NULL — scraped data is often incomplete
    city                VARCHAR(100),
    state               VARCHAR(50),
    zip_code            VARCHAR(20),
    latitude            DECIMAL(10, 8),
    longitude           DECIMAL(11, 8),
    property_type       VARCHAR(50),
    year_built          INTEGER,
    lot_size_sqft       DECIMAL(12, 2),
    living_area_sqft    DECIMAL(10, 2),
    bedrooms            INTEGER,
    bathrooms           DECIMAL(4, 2),
    listing_url         TEXT,                           -- added: direct link to source listing
    source_url          TEXT,                           -- added: which site it came from
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Price history — time-series core of the pipeline
CREATE TABLE IF NOT EXISTS price_history (
    id              SERIAL PRIMARY KEY,
    property_id     VARCHAR(255) REFERENCES properties(property_id),
    price           DECIMAL(15, 2) NOT NULL,            -- increased precision for NGN amounts (e.g. ₦380,000,000)
    listing_date    DATE NOT NULL,
    sale_date       DATE,
    price_per_sqft  DECIMAL(10, 2),
    status          VARCHAR(50),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(property_id, listing_date)
);

-- Market metrics — aggregated by geography and date
CREATE TABLE IF NOT EXISTS market_metrics (
    id                      SERIAL PRIMARY KEY,
    metric_date             DATE NOT NULL,
    city                    VARCHAR(100),
    state                   VARCHAR(50),
    zip_code                VARCHAR(20),
    median_price            DECIMAL(15, 2),
    average_price           DECIMAL(15, 2),
    median_price_per_sqft   DECIMAL(10, 2),
    inventory_count         INTEGER,
    days_on_market_avg      INTEGER,
    sales_volume            INTEGER,
    price_mom_pct           DECIMAL(5, 2),
    price_yoy_pct           DECIMAL(5, 2),
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(metric_date, zip_code)
);

-- Forecast results
CREATE TABLE IF NOT EXISTS price_forecasts (
    id                  SERIAL PRIMARY KEY,
    property_id         VARCHAR(255) REFERENCES properties(property_id),
    forecast_date       DATE NOT NULL,
    predicted_price     DECIMAL(15, 2),
    confidence_lower    DECIMAL(15, 2),
    confidence_upper    DECIMAL(15, 2),
    model_version       VARCHAR(50),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Scraper job logs
CREATE TABLE IF NOT EXISTS scraper_logs (
    id                  SERIAL PRIMARY KEY,
    job_id              VARCHAR(100),
    source_url          TEXT,
    records_scraped     INTEGER,
    status              VARCHAR(50),
    error_message       TEXT,
    started_at          TIMESTAMP,
    completed_at        TIMESTAMP,
    duration_seconds    INTEGER,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ETL job tracking
CREATE TABLE IF NOT EXISTS etl_jobs (
    id                  SERIAL PRIMARY KEY,
    job_name            VARCHAR(100),
    run_id              VARCHAR(100) UNIQUE,
    status              VARCHAR(50),
    records_processed   INTEGER,
    error_message       TEXT,
    started_at          TIMESTAMP,
    completed_at        TIMESTAMP,
    duration_seconds    INTEGER,
    metadata            JSONB,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- INDEXES
-- Time-range queries on price_history and market_metrics
-- are the hot path — index those aggressively.
-- ============================================

CREATE INDEX IF NOT EXISTS idx_properties_location
    ON properties(latitude, longitude);

CREATE INDEX IF NOT EXISTS idx_properties_zip
    ON properties(zip_code);

CREATE INDEX IF NOT EXISTS idx_properties_type
    ON properties(property_type);

CREATE INDEX IF NOT EXISTS idx_properties_source
    ON properties(source_url);

-- Covering index for the most common price history query pattern:
-- "give me all prices for property X ordered by date"
CREATE INDEX IF NOT EXISTS idx_price_history_property_date
    ON price_history(property_id, listing_date DESC);

-- For dashboard queries: "all prices in date range"
CREATE INDEX IF NOT EXISTS idx_price_history_date
    ON price_history(listing_date DESC);

CREATE INDEX IF NOT EXISTS idx_market_metrics_date
    ON market_metrics(metric_date DESC);

CREATE INDEX IF NOT EXISTS idx_market_metrics_zip_date
    ON market_metrics(zip_code, metric_date DESC);

CREATE INDEX IF NOT EXISTS idx_forecasts_property_date
    ON price_forecasts(property_id, forecast_date DESC);

-- ============================================
-- VIEWS
-- ============================================

-- Current price per property (most recent listing_date)
CREATE OR REPLACE VIEW property_current_prices AS
SELECT DISTINCT ON (p.property_id)
    p.property_id,
    p.address,
    p.city,
    p.state,
    p.zip_code,
    p.property_type,
    p.bedrooms,
    p.bathrooms,
    p.listing_url,
    ph.price           AS current_price,
    ph.listing_date    AS last_listing_date,
    ph.status
FROM properties p
LEFT JOIN price_history ph ON p.property_id = ph.property_id
ORDER BY p.property_id, ph.listing_date DESC;

-- 30-day market snapshot — used by Grafana dashboards
CREATE MATERIALIZED VIEW IF NOT EXISTS market_trends_30d AS
SELECT
    zip_code,
    metric_date,
    median_price,
    average_price,
    inventory_count,
    days_on_market_avg
FROM market_metrics
WHERE metric_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY zip_code, metric_date DESC;

-- Refresh index for the materialized view
CREATE UNIQUE INDEX IF NOT EXISTS idx_market_trends_30d_zip_date
    ON market_trends_30d(zip_code, metric_date DESC);

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
        CREATE USER grafana_reader WITH PASSWORD 'GrafanaReadOnly2024!';
    END IF;
END
$$;

-- NOTE: The DB name here must match POSTGRES_DB in your .env
-- If your DB is not named 'realestate_db', update the line below.
GRANT CONNECT ON DATABASE realestate TO grafana_reader;
GRANT USAGE ON SCHEMA public TO grafana_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_reader;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO grafana_reader;

-- Ensure future tables are also readable by grafana_reader
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO grafana_reader;