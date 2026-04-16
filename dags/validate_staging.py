import os
import sys
import time
import logging
import pandas as pd
import great_expectations as gx
import great_expectations.expectations as gxe
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# POLLING HELPER
# --------------------------------------------------------------------------
# We poll until at least one row appears (or we time out).
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

    # 4. Create Ephemeral Context & Batch (GX 1.0 Fluent API)
    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas("staging_data")
    data_asset = data_source.add_dataframe_asset(name="raw_listings")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("my_batch")
    
    # 5. Define Expectations the GX 1.0 Way
    suite = gx.ExpectationSuite(name="staging_suite")
    
    # Rule 1: Every listing MUST have a unique ID and a URL
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="property_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="url"))

    # Rule 2: We should only be scraping from our approved sources.
    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(
        column="source_site",
        value_set=["PropertyPro", "NigeriaPropertyCentre", "PrivateProperty", "Unknown"]
    ))

    # Rule 3: Bedrooms and Bathrooms shouldn't be negative
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(
        column="bedrooms", min_value=0, max_value=50, mostly=0.95
    ))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(
        column="bathrooms", min_value=0, max_value=50, mostly=0.95
    ))

    # Rule 4: The extracted_at timestamp must exist so dbt can order it
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="extracted_at"))

    # Rule 5: price_raw must be present for the majority of listings
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="price_raw", mostly=0.7))

    # ---> THE FIX: Register the suite with the context <---
    context.suites.add(suite)

    # 6. Create Validation Definition
    validation_definition = gx.ValidationDefinition(
        name="staging_validation",
        data=batch_definition,
        suite=suite
    )
    
    # ---> THE FIX: Register the validation definition with the context <---
    context.validation_definitions.add(validation_definition)

    # 7. Run Validation
    results = validation_definition.run(batch_parameters={"dataframe": df})

    # --- EVALUATE ---
    
    # In GX 1.0, results is an object, not a dictionary.
    failed_expectations = [r for r in results.results if not r.success]

    if failed_expectations:
        logger.error(f"❌ VALIDATION FAILED: {len(failed_expectations)} expectations failed.")
        for failure in failed_expectations:
            col = failure.expectation_config.kwargs.get("column", "Unknown")
            rule = failure.expectation_config.type
            percent_missing = failure.result.get("unexpected_percent", 0)
            logger.error(f" - Column '{col}' failed '{rule}'. ({percent_missing:.1f}% unexpected)")

        raise ValueError("Data validation failed! Pipeline halted.")

    else:
        logger.info(f"✅ VALIDATION PASSED! All expectations met on {len(df)} rows.")


if __name__ == "__main__":
    run_validation()