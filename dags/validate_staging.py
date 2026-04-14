import os
import sys
import time
import logging
import pandas as pd
import great_expectations as gx
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# POLLING HELPER
# ---------------------------------------------------------------------------
# FIX: spark_flatten_staging submits the Spark job via the REST API and returns
# immediately (fire-and-forget). GX was therefore querying stg_raw_listings
# before Spark had written anything, producing "Staging table is empty" on
# every run and silently skipping all validation.
#
# We now poll until at least one row appears (or we time out). A 5-minute
# window with 15-second sleeps is generous; the ETL job usually finishes in
# 60-120 seconds on a small dataset.
# ---------------------------------------------------------------------------
def wait_for_data(engine, timeout_seconds=300, poll_interval=15):
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM staging.stg_raw_listings")).scalar()
        if result > 0:
            logger.info(f"Staging table has {result} rows — proceeding with validation.")
            return True
        remaining = int(deadline - time.time())
        logger.info(f"Staging table still empty; waiting {poll_interval}s (timeout in {remaining}s)…")
        time.sleep(poll_interval)

    logger.error("Timed out waiting for Spark to populate staging.stg_raw_listings.")
    return False


def run_validation():
    logger.info("Starting Great Expectations validation on staging data...")

    # 1. Connect to Postgres
    dsn = os.getenv('POSTGRES_DSN')
    engine = create_engine(dsn)

    # 2. Wait for Spark to finish writing (see comment above)
    if not wait_for_data(engine):
        sys.exit(1)

    # 3. Load the newly flattened staging data
    try:
        df = pd.read_sql("SELECT * FROM staging.stg_raw_listings", engine)
    except Exception as e:
        logger.error(f"Failed to read staging table. Error: {e}")
        sys.exit(1)

    if df.empty:
        logger.warning("Staging table is empty after polling timeout. Skipping validation.")
        return

    # 4. Wrap the Pandas DataFrame in Great Expectations
    gx_df = gx.from_pandas(df)

    results = []

    # --- DEFINE OUR EXPECTATIONS ---

    # Rule 1: Every listing MUST have a unique ID and a URL
    results.append(gx_df.expect_column_values_to_not_be_null("property_id"))
    results.append(gx_df.expect_column_values_to_not_be_null("url"))

    # Rule 2: We should only be scraping from our approved sources.
    # FIX: The ETL writes short display names ('PropertyPro', 'NigeriaPropertyCentre',
    # 'PrivateProperty'), NOT domain names. The old set used domain strings, which
    # caused this expectation to fail for every single row in the table.
    results.append(gx_df.expect_column_values_to_be_in_set(
        "source_site",
        ["PropertyPro", "NigeriaPropertyCentre", "PrivateProperty", "Unknown"]
    ))

    # Rule 3: Bedrooms and Bathrooms shouldn't be negative
    results.append(gx_df.expect_column_values_to_be_between(
        "bedrooms", min_value=0, max_value=50, mostly=0.95
    ))
    results.append(gx_df.expect_column_values_to_be_between(
        "bathrooms", min_value=0, max_value=50, mostly=0.95
    ))

    # Rule 4: The extracted_at timestamp must exist so dbt can order it
    results.append(gx_df.expect_column_values_to_not_be_null("extracted_at"))

    # Rule 5: price_raw must be present for the majority of listings
    # (some listings legitimately say "Price on Request", so we use mostly=0.7)
    results.append(gx_df.expect_column_values_to_not_be_null("price_raw", mostly=0.7))

    # --- EVALUATE ---

    failed_expectations = [r for r in results if not r["success"]]

    if failed_expectations:
        logger.error(f"❌ VALIDATION FAILED: {len(failed_expectations)} expectations failed.")
        for failure in failed_expectations:
            col = failure["expectation_config"]["kwargs"].get("column", "Unknown")
            rule = failure["expectation_config"]["expectation_type"]
            percent_missing = failure["result"].get("unexpected_percent", 0)
            logger.error(f" - Column '{col}' failed '{rule}'. ({percent_missing:.1f}% unexpected)")

        raise ValueError("Data validation failed! Pipeline halted.")

    else:
        logger.info(f"✅ VALIDATION PASSED! {len(results)} expectations met on {len(df)} rows.")


if __name__ == "__main__":
    run_validation()