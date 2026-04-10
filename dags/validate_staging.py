import os
import sys
import logging
import pandas as pd
import great_expectations as gx
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_validation():
    logger.info("Starting Great Expectations validation on staging data...")
    
    # 1. Connect to Postgres
    dsn = os.getenv('POSTGRES_DSN')
    engine = create_engine(dsn)
    
    # 2. Load the newly flattened staging data
    try:
        df = pd.read_sql("SELECT * FROM staging.stg_raw_listings", engine)
    except Exception as e:
        logger.error(f"Failed to read staging table. Has Spark run yet? Error: {e}")
        sys.exit(1)

    if df.empty:
        logger.warning("Staging table is empty. Skipping validation.")
        return

    # 3. Wrap the Pandas DataFrame in Great Expectations
    gx_df = gx.from_pandas(df)
    
    results = []
    
    # --- DEFINE OUR EXPECTATIONS ---
    
    # Rule 1: Every listing MUST have a unique ID and a URL
    results.append(gx_df.expect_column_values_to_not_be_null("property_id"))
    results.append(gx_df.expect_column_values_to_not_be_null("url"))
    
    # Rule 2: We should only be scraping from our approved sources
    results.append(gx_df.expect_column_values_to_be_in_set(
        "source_site", 
        ["propertypro.ng", "nigeriapropertycentre.com", "privateproperty.ng", "Unknown"]
    ))
    
    # Rule 3: Bedrooms and Bathrooms shouldn't be negative
    results.append(gx_df.expect_column_values_to_be_between(
        "bedrooms", min_value=0, max_value=50, mostly=0.95  # mostly=0.95 means we allow 5% to be null/missing
    ))
    results.append(gx_df.expect_column_values_to_be_between(
        "bathrooms", min_value=0, max_value=50, mostly=0.95
    ))
    
    # Rule 4: The extracted_at timestamp must exist so dbt can order it
    results.append(gx_df.expect_column_values_to_not_be_null("extracted_at"))
    
    # --- EVALUATE ---
    
    failed_expectations = [r for r in results if not r["success"]]
    
    if failed_expectations:
        logger.error(f"❌ VALIDATION FAILED: {len(failed_expectations)} expectations failed.")
        for failure in failed_expectations:
            # Print exactly what failed and the unexpected values
            col = failure["expectation_config"]["kwargs"].get("column", "Unknown")
            rule = failure["expectation_config"]["expectation_type"]
            percent_missing = failure["result"].get("unexpected_percent", 0)
            logger.error(f" - Column '{col}' failed '{rule}'. ({percent_missing}% unexpected)")
        
        # We explicitly raise an exception here so the Airflow task FAILS.
        # This prevents the downstream dbt task from running and polluting the Gold tables.
        raise ValueError("Data validation failed! Pipeline halted.")
        
    else:
        logger.info(f"✅ VALIDATION PASSED! {len(results)} expectations met on {len(df)} rows.")

if __name__ == "__main__":
    run_validation()