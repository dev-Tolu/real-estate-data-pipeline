from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, stddev, lit, to_date, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
import logging
import os
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RealEstateETL:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("RealEstateETL") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        # MinIO/S3 configuration
        self.minio_endpoint = os.getenv('MINIO_ENDPOINT', 'minio:9000')
        self.bucket_raw = os.getenv('S3_BUCKET_RAW', 'realestate-raw-data')
        self.bucket_processed = os.getenv('S3_BUCKET_PROCESSED', 'realestate-processed-data')
        self.bucket_analytics = os.getenv('S3_BUCKET_ANALYTICS', 'realestate-analytics')
        
        # Configure Spark to read from MinIO
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", f"http://{self.minio_endpoint}")
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.getenv('MINIO_ROOT_USER'))
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.getenv('MINIO_ROOT_PASSWORD'))
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    def read_raw_data(self):
        """Read raw JSON data from MinIO"""
        try:
            path = f"s3a://{self.bucket_raw}/*.json"
            logger.info(f"Reading data from {path}")
            
            df = self.spark.read.json(path)
            logger.info(f"Read {df.count()} raw records")
            return df
        except Exception as e:
            logger.error(f"Failed to read raw data: {e}")
            return None
    
    def clean_data(self, df):
        """Clean and validate the data"""
        logger.info("Cleaning data...")
        
        # Remove duplicates
        df = df.dropDuplicates(['property_id', 'listing_date'])
        
        # Drop rows with null essential fields
        df = df.dropna(subset=['property_id', 'price'])
        
        # Clean price column (remove outliers)
        price_stats = df.select(avg('price'), stddev('price')).collect()
        avg_price = price_stats[0][0]
        std_price = price_stats[0][1]
        
        if avg_price and std_price:
            df = df.filter(
                (col('price') > 0) & 
                (col('price') < (avg_price + 3 * std_price))
            )
        
        # Clean text fields
        df = df.withColumn('property_type', 
            when(col('property_type').isNull(), 'unknown')
            .otherwise(col('property_type'))
        )
        
        # Convert data types
        df = df.withColumn('price', col('price').cast(DoubleType())) \
               .withColumn('bedrooms', col('bedrooms').cast(IntegerType())) \
               .withColumn('bathrooms', col('bathrooms').cast(DoubleType())) \
               .withColumn('sqft', col('sqft').cast(IntegerType())) \
               .withColumn('listing_date', to_date(col('listing_date')))
        
        logger.info(f"Cleaned data: {df.count()} records remaining")
        return df
    
    def enrich_data(self, df):
        """Enrich data with additional features"""
        logger.info("Enriching data...")
        
        # Calculate price per square foot
        df = df.withColumn('price_per_sqft', 
            when(col('sqft') > 0, col('price') / col('sqft'))
            .otherwise(lit(None))
        )
        
        # Create price tier categories
        df = df.withColumn('price_tier',
            when(col('price') < 300000, 'budget')
            .when((col('price') >= 300000) & (col('price') < 600000), 'mid-range')
            .when((col('price') >= 600000) & (col('price') < 1000000), 'luxury')
            .otherwise('ultra-luxury')
        )
        
        # Add month and year columns for aggregation
        df = df.withColumn('year', col('listing_date').substr(1, 4)) \
               .withColumn('month', col('listing_date').substr(6, 2))
        
        logger.info("Data enrichment complete")
        return df
    
    def aggregate_market_metrics(self, df):
        """Calculate market-level aggregates"""
        logger.info("Calculating market metrics...")
        
        # Aggregate by zip code and date
        market_metrics = df.groupBy('zip_code', 'listing_date').agg(
            avg('price').alias('average_price'),
            avg('price_per_sqft').alias('avg_price_per_sqft'),
            count('property_id').alias('inventory_count')
        )
        
        # Add rolling averages (requires window functions)
        from pyspark.sql.window import Window
        window_spec = Window.partitionBy('zip_code').orderBy('listing_date').rowsBetween(-29, 0)
        
        market_metrics = market_metrics.withColumn(
            'rolling_30d_avg_price',
            avg('average_price').over(window_spec)
        )
        
        logger.info("Market metrics calculated")
        return market_metrics
    
    def save_processed_data(self, df, market_metrics):
        """Save processed data back to MinIO"""
        logger.info("Saving processed data...")
        
        # Save cleaned and enriched data
        output_path = f"s3a://{self.bucket_processed}/enriched_data_{datetime.now().strftime('%Y%m%d')}"
        df.write.mode("overwrite").parquet(output_path)
        logger.info(f"Saved enriched data to {output_path}")
        
        # Save market metrics
        metrics_path = f"s3a://{self.bucket_analytics}/market_metrics_{datetime.now().strftime('%Y%m%d')}"
        market_metrics.write.mode("overwrite").parquet(metrics_path)
        logger.info(f"Saved market metrics to {metrics_path}")
        
        # Also save as CSV for easy access (optional)
        csv_path = f"s3a://{self.bucket_analytics}/market_metrics_{datetime.now().strftime('%Y%m%d')}.csv"
        market_metrics.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)
        
        return output_path, metrics_path
    
    def write_to_postgres(self, df, table_name):
        """Write processed data to PostgreSQL"""
        logger.info(f"Writing to PostgreSQL table: {table_name}")
        
        jdbc_url = os.getenv('POSTGRES_DSN')
        connection_properties = {
            "user": os.getenv('POSTGRES_USER'),
            "password": os.getenv('POSTGRES_PASSWORD'),
            "driver": "org.postgresql.Driver"
        }
        
        df.write \
            .mode("append") \
            .jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
        
        logger.info(f"Successfully wrote to {table_name}")
    
    def run(self):
        """Main ETL orchestration"""
        try:
            # 1. Read raw data
            raw_df = self.read_raw_data()
            if raw_df is None:
                raise Exception("Failed to read raw data")
            
            # 2. Clean data
            clean_df = self.clean_data(raw_df)
            
            # 3. Enrich data
            enriched_df = self.enrich_data(clean_df)
            
            # 4. Calculate market metrics
            market_metrics = self.aggregate_market_metrics(enriched_df)
            
            # 5. Save to data lake
            output_path, metrics_path = self.save_processed_data(enriched_df, market_metrics)
            
            # 6. Write to PostgreSQL (optional)
            # self.write_to_postgres(enriched_df, "price_history")
            
            logger.info("ETL pipeline completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            raise
        finally:
            self.spark.stop()

if __name__ == "__main__":
    etl = RealEstateETL()
    etl.run()