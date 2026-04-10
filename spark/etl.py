import os
import json
import hashlib
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode, current_timestamp, udf
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, FloatType, IntegerType
from bs4 import BeautifulSoup
import re

# ---------------------------------------------------------
# 1. Define the Schema for parsed listings
# ---------------------------------------------------------
listing_schema = StructType([
    StructField("property_id", StringType(), True),
    StructField("url", StringType(), True),
    StructField("title", StringType(), True),
    StructField("price_raw", StringType(), True),
    StructField("location", StringType(), True),
    StructField("bedrooms", IntegerType(), True),
    StructField("bathrooms", IntegerType(), True),
    StructField("source_site", StringType(), True)
])

# ---------------------------------------------------------
# 2. Spark UDF: The BeautifulSoup Parser
# ---------------------------------------------------------
# This function runs on your Spark worker nodes. 
# It takes a massive HTML string and returns an array of structured dictionaries.
def parse_html_to_listings(html: str, source_url: str):
    if not html: return []

    # --- Helper Functions ---
    def _text(el): return el.get_text(strip=True) if el else None
    def _parse_int(text):
        if not text: return None
        digits = ''.join(c for c in text if c.isdigit())
        return digits if digits else None

    soup = BeautifulSoup(html, 'html.parser')
    listings = []

    # --- ROUTING LOGIC ---
    
    if 'propertypro.ng' in source_url:
        source_site = 'PropertyPro'
        cards = soup.select('div.property-listing, div.property-listing-grid')
        for card in cards:
            try:
                title_el = card.select_one('.pl-title h3 a, .property-listing-content h3 a, h3 a')
                price_el = card.select_one('.pl-price h3, [class*="price"] h3')
                location_el = card.select_one('.pl-title p, .property-listing-content address, [class*="location"]')
                details_el = card.select_one('.pl-price h6, .property-listing-content h6')
                
                # Extract beds/baths text
                beds, baths = None, None
                if details_el:
                    details_text = _text(details_el)
                    beds_match = re.search(r'(\d+)\s*Bed', details_text, re.IGNORECASE)
                    baths_match = re.search(r'(\d+)\s*Bath', details_text, re.IGNORECASE)
                    beds = beds_match.group(1) if beds_match else None
                    baths = baths_match.group(1) if baths_match else None

                # Get URL
                link_el = card.select_one('.pl-title h3 a, .property-listing-content h3 a, a[href*="/property/"]')
                url = link_el.get('href', '') if link_el else ''
                if url and not url.startswith('http'):
                    url = f"https://propertypro.ng{url}"

                # Generate ID
                identifier = f"{source_site}_{url}_{_text(title_el)}".lower()
                prop_id = hashlib.md5(identifier.encode()).hexdigest()[:32]

                listings.append({
                    "property_id": prop_id,
                    "url": url,
                    "title": _text(title_el),
                    "price_raw": _text(price_el),
                    "currency_raw": "NGN",
                    "location": _text(location_el),
                    "bedrooms": beds,
                    "bathrooms": baths,
                    "sqft": None,
                    "property_type": "unknown", # Let dbt regex the title later
                    "source_site": source_site
                })
            except Exception:
                continue

    elif 'privateproperty.ng' in source_url:
        source_site = 'PrivateProperty'
        cards = soup.select('div.similar-listings-item, div.result-listings > div')
        for card in cards:
            try:
                title_el = card.select_one('.similar-listings-info h2 a, .similar-listings-info h2')
                price_el = card.select_one('.similar-listings-price h4')
                location_el = card.select_one('.listings-location')
                
                price_text = _text(price_el)
                currency = 'USD' if price_text and '$' in price_text else 'NGN'

                # Extract beds
                beds, baths = None, None
                full_text = card.get_text().lower()
                beds_match = re.search(r'(\d+)\s*bed', full_text)
                if beds_match: beds = beds_match.group(1)

                link_el = card.select_one('.similar-listings-info h2 a, a[href*="/listings/"]')
                url = link_el.get('href', '') if link_el else ''
                if url and not url.startswith('http'):
                    url = f"https://privateproperty.ng{url}"

                identifier = f"{source_site}_{url}_{_text(title_el)}".lower()
                prop_id = hashlib.md5(identifier.encode()).hexdigest()[:32]

                listings.append({
                    "property_id": prop_id,
                    "url": url,
                    "title": _text(title_el),
                    "price_raw": price_text,
                    "currency_raw": currency,
                    "location": _text(location_el),
                    "bedrooms": beds,
                    "bathrooms": baths,
                    "sqft": None,
                    "property_type": "unknown",
                    "source_site": source_site
                })
            except Exception:
                continue
                
    elif 'nigeriapropertycentre.com' in source_url:
        source_site = 'NigeriaPropertyCentre'
        cards = soup.select('div.row.property-list, div.wp-block.property.list')
        for card in cards:
            try:
                title_el = card.select_one('h3, h4, .content-title, [itemprop="name"]')
                price_el = card.select_one('span.price, [class*="price"], .similar-listings-price h4, .pl-price h3, h4 span[content]')
                location_el = card.select_one('address, [class*="location"], [class*="address"]')
                
                # Extract beds and baths from specific <li> elements
                beds, baths = None, None
                beds_li = card.select_one('li i.fa-bed, li i.fal.fa-bed')
                if beds_li and beds_li.find_parent('li'):
                    beds = _text(beds_li.find_parent('li').select_one('span'))
                
                baths_li = card.select_one('li i.fa-bath, li i.fal.fa-bath')
                if baths_li and baths_li.find_parent('li'):
                    baths = _text(baths_li.find_parent('li').select_one('span'))

                link_el = card.select_one('a[href*="/for-sale/"], a[href*="/property/"]')
                url = link_el.get('href', '') if link_el else ''
                if url and not url.startswith('http'):
                    url = f"https://www.nigeriapropertycentre.com{url}"

                identifier = f"{source_site}_{url}_{_text(title_el)}".lower()
                prop_id = hashlib.md5(identifier.encode()).hexdigest()[:32]

                listings.append({
                    "property_id": prop_id,
                    "url": url,
                    "title": _text(title_el),
                    "price_raw": _text(price_el),
                    "currency_raw": "NGN",
                    "location": _text(location_el),
                    "bedrooms": beds,
                    "bathrooms": baths,
                    "sqft": None,
                    "property_type": "unknown",
                    "source_site": source_site
                })
            except Exception:
                continue

    else:
        # GENERIC FALLBACK
        source_site = 'Unknown'
        cards = soup.select('.listing, .property-card, .property-item, article.property, div[class*="listing"]')
        for card in cards:
            try:
                link_el = card.select_one('a')
                url = link_el.get('href', '') if link_el else ''
                if url and not url.startswith('http'):
                    url = source_url.rstrip('/') + '/' + url.lstrip('/')

                title_el = card.select_one('h1,h2,h3,h4')
                identifier = f"{source_site}_{url}_{_text(title_el)}".lower()
                prop_id = hashlib.md5(identifier.encode()).hexdigest()[:32]

                listings.append({
                    "property_id": prop_id,
                    "url": url,
                    "title": _text(title_el),
                    "price_raw": _text(card.select_one('.price,[class*="price"]')),
                    "currency_raw": "NGN",
                    "location": _text(card.select_one('.location,.address,[class*="location"]')),
                    "bedrooms": _text(card.select_one('.beds,[class*="bed"]')),
                    "bathrooms": _text(card.select_one('.baths,[class*="bath"]')),
                    "sqft": _text(card.select_one('.sqft,[class*="area"]')),
                    "property_type": _text(card.select_one('.type,.tag,[class*="type"]')) or 'unknown',
                    "source_site": source_site
                })
            except Exception:
                continue

    return listings

# Register the UDF
parse_udf = udf(parse_html_to_listings, ArrayType(listing_schema))

# ---------------------------------------------------------
# 3. Main Spark Pipeline
# ---------------------------------------------------------
def run_etl():
    # Initialize Spark with MinIO (S3A) and Postgres JDBC configurations
    spark = SparkSession.builder \
        .appName("RealEstate_Bronze_to_Silver") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv('MINIO_ROOT_USER')) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv('MINIO_ROOT_PASSWORD')) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    bucket = os.getenv('S3_BUCKET_RAW', 'realestate-raw-data')
    
    # 1. Read Raw JSON from MinIO
    df_raw = spark.read.option("multiline", "true").json(f"s3a://{bucket}/*.json")
    
    # 2. Apply the parsing UDF (Creates a column of arrays)
    df_parsed = df_raw.withColumn("parsed_data", parse_udf(col("html"), col("source_url")))
    
    # 3. Explode the array so each listing gets its own row
    df_exploded = df_parsed.select(explode(col("parsed_data")).alias("listing"), col("scraped_at"))
    
    # 4. Flatten the struct into top-level columns
    df_flattened = df_exploded.select(
        col("listing.property_id"),
        col("listing.url"),
        col("listing.title"),
        col("listing.price_raw"),
        col("listing.currency_raw"),
        col("listing.location"),
        col("listing.bedrooms"),
        col("listing.bathrooms"),
        col("listing.sqft"),
        col("listing.property_type"),
        col("listing.source_site"),
        col("scraped_at").alias("extracted_at")
    ).withColumn("spark_processed_at", current_timestamp())

    # 5. Write to PostgreSQL Staging Schema
    postgres_url = f"jdbc:postgresql://postgres:5432/{os.getenv('POSTGRES_DB')}"
    postgres_properties = {
        "user": os.getenv('POSTGRES_USER'),
        "password": os.getenv('POSTGRES_PASSWORD'),
        "driver": "org.postgresql.Driver"
    }

    df_flattened.write \
        .jdbc(url=postgres_url, 
              table="staging.stg_raw_listings", 
              mode="append", 
              properties=postgres_properties)

    spark.stop()

if __name__ == "__main__":
    run_etl()