#!/usr/bin/env python3
import os
import json
import hashlib
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode, current_timestamp
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, IntegerType
from bs4 import BeautifulSoup
import re

# ---------------------------------------------------------
# 1. Define the Schema for parsed listings
# ---------------------------------------------------------
# FIX: Added currency_raw, sqft, property_type — previously absent from the schema,
# causing Spark to silently drop all three columns from every parsed listing.
listing_schema = StructType([
    StructField("property_id",   StringType(),  True),
    StructField("url",           StringType(),  True),
    StructField("title",         StringType(),  True),
    StructField("price_raw",     StringType(),  True),
    StructField("currency_raw",  StringType(),  True),
    StructField("location",      StringType(),  True),
    StructField("bedrooms",      IntegerType(), True),
    StructField("bathrooms",     IntegerType(), True),
    StructField("sqft",          StringType(),  True),
    StructField("property_type", StringType(),  True),
    StructField("source_site",   StringType(),  True),
])

# ---------------------------------------------------------
# 2. Spark UDF: The BeautifulSoup Parser
# ---------------------------------------------------------
def parse_html_to_listings(html: str, source_url: str):
    if not html:
        return []

    # --- Helper Functions ---
    def _text(el):
        return el.get_text(strip=True) if el else None

    # FIX: _parse_int now returns int (not str) to match IntegerType schema.
    # Previously returned a string like "3", causing a schema type mismatch.
    def _parse_int(text):
        if not text:
            return None
        digits = ''.join(c for c in str(text) if c.isdigit())
        return int(digits) if digits else None

    def extract_type_from_url(url_string):
        if not url_string:
            return None
        url_lower = url_string.lower()

        # 1. Apartments / Flats / Rooms
        if 'block-of-flats'    in url_lower: return 'Block Of Flats'
        if 'mini-flat'         in url_lower: return 'Mini Flat'
        if 'self-contain'      in url_lower: return 'Self Contain'
        if 'shared-apartment'  in url_lower: return 'Shared Apartment'
        if 'studio-apartment'  in url_lower: return 'Studio Apartment'
        if 'penthouse'         in url_lower: return 'Penthouse'
        # FIX: was `'room' in url_lower` which matches 'bedroom' in every URL.
        # Now uses the slug that property sites actually emit for this category.
        if 'rooms-and-boys-quarter' in url_lower or 'boys-quarter' in url_lower:
            return 'Rooms & Boys Quarters'
        if 'flat' in url_lower or 'apartment' in url_lower: return 'Flats & Apartments'

        # 2. Houses / Duplexes / Bungalows
        if 'semi-detached-duplex'  in url_lower: return 'Semi-Detached Duplex'
        if 'detached-duplex'       in url_lower: return 'Detached Duplex'
        if 'terraced-duplex'       in url_lower: return 'Terraced Duplex'
        if 'terrace'               in url_lower: return 'Terraced Duplex'
        if 'duplex'                in url_lower: return 'Duplex'

        if 'semi-detached-bungalow' in url_lower: return 'Semi-Detached Bungalow'
        if 'detached-bungalow'      in url_lower: return 'Detached Bungalow'
        if 'terraced-bungalow'      in url_lower: return 'Terraced Bungalow'
        if 'bungalow'               in url_lower: return 'Bungalow'

        if 'townhouse'  in url_lower: return 'Townhouse'
        if 'maisonette' in url_lower: return 'Maisonette'
        if 'house'      in url_lower: return 'House'

        # 3. Land
        if 'commercial-land'    in url_lower: return 'Commercial Land'
        if 'industrial-land'    in url_lower: return 'Industrial Land'
        if 'joint-venture-land' in url_lower: return 'Joint Venture Land'
        if 'mixed-use-land'     in url_lower: return 'Mixed-use Land'
        if 'residential-land'   in url_lower: return 'Residential Land'
        if 'land'  in url_lower: return 'Land'
        if 'plot'  in url_lower: return 'Land'

        # 4. Commercial / Other
        if 'co-working-space'  in url_lower: return 'Co-working Space'
        if 'conference-room'   in url_lower: return 'Conference/Meeting Room'
        if 'meeting-room'      in url_lower: return 'Conference/Meeting Room'
        if 'desk'              in url_lower: return 'Desk/Workstation'
        if 'workstation'       in url_lower: return 'Desk/Workstation'
        if 'event-hall'        in url_lower: return 'Event Hall'
        if 'factory'           in url_lower: return 'Factory'
        if 'filling-station'   in url_lower: return 'Oil & Gas Property'
        if 'gas-plant'         in url_lower: return 'Oil & Gas Property'
        if 'tank-farm'         in url_lower: return 'Oil & Gas Property'
        if 'hostel'            in url_lower: return 'Hostel'
        if 'hotel'             in url_lower: return 'Hotel'
        if 'mall'              in url_lower: return 'Mall/Complex/Plaza'
        if 'office'            in url_lower: return 'Office'
        if 'restaurant-bar'    in url_lower: return 'Restaurant/Bar'
        if 'school'            in url_lower: return 'School'
        if 'shop'              in url_lower: return 'Shop'
        if 'warehouse'         in url_lower: return 'Warehouse'
        if 'commercial'        in url_lower: return 'Commercial Property'

        return None

    soup = BeautifulSoup(html, 'html.parser')
    listings = []

    # --- ROUTING LOGIC ---

    if 'propertypro.ng' in source_url:
        source_site = 'PropertyPro'
        # FIX: was `'div.property-listing, div.property-listing-grid'`
        # div.property-listing-grid is always nested inside div.property-listing,
        # so the old compound selector matched both wrapper and child — producing
        # exactly 2× the real listing count per page.
        cards = soup.select('div.property-listing')
        for card in cards:
            try:
                title_el   = card.select_one('.pl-title h3 a, .property-listing-content h3 a, h3 a')
                price_el   = card.select_one('.pl-price h3, [class*="price"] h3')
                location_el= card.select_one('.pl-title p, .property-listing-content address, [class*="location"]')
                details_el = card.select_one('.pl-price h6, .property-listing-content h6')
                type_badge = card.select_one('.property-features span, .pl-title h4')
                prop_type  = _text(type_badge)

                beds, baths = None, None
                if details_el:
                    details_text = _text(details_el)
                    beds_match  = re.search(r'(\d+)\s*Bed',  details_text, re.IGNORECASE)
                    baths_match = re.search(r'(\d+)\s*Bath', details_text, re.IGNORECASE)
                    # FIX: wrap with _parse_int so we return int, not str
                    beds  = _parse_int(beds_match.group(1))  if beds_match  else None
                    baths = _parse_int(baths_match.group(1)) if baths_match else None

                link_el = card.select_one('.pl-title h3 a, .property-listing-content h3 a, a[href*="/property/"]')
                url = link_el.get('href', '') if link_el else ''
                if url and not url.startswith('http'):
                    url = f"https://propertypro.ng{url}"

                identifier = f"{source_site}_{url}_{_text(title_el)}".lower()
                prop_id = hashlib.md5(identifier.encode()).hexdigest()[:32]

                if not prop_type:
                    prop_type = extract_type_from_url(url)

                listings.append({
                    "property_id":   prop_id,
                    "url":           url,
                    "title":         _text(title_el),
                    "price_raw":     _text(price_el),
                    "currency_raw":  "NGN",
                    "location":      _text(location_el),
                    "bedrooms":      beds,
                    "bathrooms":     baths,
                    "sqft":          None,
                    "property_type": prop_type or "unknown",
                    "source_site":   source_site,
                })
            except Exception:
                continue

    elif 'privateproperty.ng' in source_url:
        source_site = 'PrivateProperty'
        cards = soup.select('div.similar-listings-item')
        for card in cards:
            try:
                title_el    = card.select_one('.similar-listings-info h2 a, .similar-listings-info h2')
                price_el    = card.select_one('.similar-listings-price h4')
                location_el = card.select_one('.listings-location')
                type_el     = card.select_one('.listings-type, .property-type')
                prop_type   = _text(type_el)

                price_text = _text(price_el)
                currency = 'USD' if price_text and '$' in price_text else 'NGN'

                beds, baths = None, None
                # FIX: PrivateProperty cards use inline SVG icons with no text labels
                # (no aria-label, no fa-* class, no 'bath' word in card text at all).
                # The feature counts live in ul.property-benefit > li, positionally:
                #   li[0] = bedrooms, li[1] = bathrooms, li[2] = parking/toilets.
                # Confirmed consistent across all pages by cross-referencing li[0]
                # against the bedroom count in the listing title.
                # Fallback to regex on full_text for any cards missing the benefit list.
                benefit_lis = card.select('ul.property-benefit li')
                if len(benefit_lis) >= 2:
                    beds  = _parse_int(_text(benefit_lis[0]))
                    baths = _parse_int(_text(benefit_lis[1]))
                else:
                    full_text   = card.get_text().lower()
                    beds_match  = re.search(r'(\d+)\s*bed', full_text)
                    if beds_match: beds = _parse_int(beds_match.group(1))

                link_el = card.select_one('.similar-listings-info h2 a, a[href*="/listings/"]')
                url = link_el.get('href', '') if link_el else ''
                if url and not url.startswith('http'):
                    url = f"https://privateproperty.ng{url}"

                identifier = f"{source_site}_{url}_{_text(title_el)}".lower()
                prop_id = hashlib.md5(identifier.encode()).hexdigest()[:32]

                if not prop_type:
                    prop_type = extract_type_from_url(url)

                listings.append({
                    "property_id":   prop_id,
                    "url":           url,
                    "title":         _text(title_el),
                    "price_raw":     price_text,
                    "currency_raw":  currency,
                    "location":      _text(location_el),
                    "bedrooms":      beds,
                    "bathrooms":     baths,
                    "sqft":          None,
                    "property_type": prop_type or "unknown",
                    "source_site":   source_site,
                })
            except Exception:
                continue

    elif 'nigeriapropertycentre.com' in source_url:
        source_site = 'NigeriaPropertyCentre'
        # FIX: was `'div.row.property-list, div.wp-block.property.list'`
        # div.wp-block.property.list is always nested inside div.row.property-list,
        # so the old selector doubled every listing. Use only the outer wrapper.
        cards = soup.select('div.row.property-list')
        for card in cards:
            try:
                title_el    = card.select_one('h3, h4, .content-title, [itemprop="name"]')
                location_el = card.select_one('address, [class*="location"], [class*="address"]')

                # FIX: the price is split across two sibling <span class="price"> elements:
                # one contains '₦' and the next contains the number.
                # select_one() only grabbed the first (the currency symbol).
                # Now we join all price spans to get '₦850,000,000'.
                price_spans = card.select('span.price')
                price_raw = ''.join(_text(s) or '' for s in price_spans).strip() or None

                beds, baths = None, None
                beds_li = card.select_one('li i.fa-bed, li i.fal.fa-bed')
                if beds_li and beds_li.find_parent('li'):
                    # FIX: wrap with _parse_int so we return int, not str
                    beds = _parse_int(_text(beds_li.find_parent('li').select_one('span')))

                baths_li = card.select_one('li i.fa-bath, li i.fal.fa-bath')
                if baths_li and baths_li.find_parent('li'):
                    baths = _parse_int(_text(baths_li.find_parent('li').select_one('span')))

                link_el = card.select_one('a[href*="/for-sale/"], a[href*="/property/"]')
                url = link_el.get('href', '') if link_el else ''
                if url and not url.startswith('http'):
                    url = f"https://www.nigeriapropertycentre.com{url}"

                # NPC encodes the full property type in its URL hierarchy
                # (e.g. /for-sale/houses/semi-detached-duplexes/...) so
                # extract_type_from_url is reliable here and replaces the
                # `small.text-muted` selector which matched nothing in practice.
                prop_type = extract_type_from_url(url)

                identifier = f"{source_site}_{url}_{_text(title_el)}".lower()
                prop_id = hashlib.md5(identifier.encode()).hexdigest()[:32]

                listings.append({
                    "property_id":   prop_id,
                    "url":           url,
                    "title":         _text(title_el),
                    "price_raw":     price_raw,
                    "currency_raw":  "NGN",
                    "location":      _text(location_el),
                    "bedrooms":      beds,
                    "bathrooms":     baths,
                    "sqft":          None,
                    "property_type": prop_type or "unknown",
                    "source_site":   source_site,
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

                title_el   = card.select_one('h1,h2,h3,h4')
                identifier = f"{source_site}_{url}_{_text(title_el)}".lower()
                prop_id    = hashlib.md5(identifier.encode()).hexdigest()[:32]

                listings.append({
                    "property_id":   prop_id,
                    "url":           url,
                    "title":         _text(title_el),
                    "price_raw":     _text(card.select_one('.price,[class*="price"]')),
                    "currency_raw":  "NGN",
                    "location":      _text(card.select_one('.location,.address,[class*="location"]')),
                    "bedrooms":      _parse_int(_text(card.select_one('.beds,[class*="bed"]'))),
                    "bathrooms":     _parse_int(_text(card.select_one('.baths,[class*="bath"]'))),
                    "sqft":          _text(card.select_one('.sqft,[class*="area"]')),
                    "property_type": _text(card.select_one('.type,.tag,[class*="type"]')) or 'unknown',
                    "source_site":   source_site,
                })
            except Exception:
                continue

    return listings


# FIX: removed duplicate `udf` import (was imported twice in original)
parse_udf = udf(parse_html_to_listings, ArrayType(listing_schema))


# ---------------------------------------------------------
# 3. Main Spark Pipeline
# ---------------------------------------------------------
def run_etl():
    spark = SparkSession.builder \
        .appName("RealEstate_Bronze_to_Silver") \
        .config("spark.hadoop.fs.s3a.endpoint",        f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}") \
        .config("spark.hadoop.fs.s3a.access.key",      os.getenv('MINIO_ROOT_USER')) \
        .config("spark.hadoop.fs.s3a.secret.key",      os.getenv('MINIO_ROOT_PASSWORD')) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl",            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages",                 "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    bucket = os.getenv('S3_BUCKET_RAW', 'realestate-raw-data')

    # 1. Read Raw JSON from MinIO
    df_raw = spark.read.option("multiline", "true").json(f"s3a://{bucket}/*.json")

    # 2. Parse HTML via UDF — returns an array of listing structs per page
    df_parsed = df_raw.withColumn("parsed_data", parse_udf(col("html"), col("source_url")))

    # 3. Explode so each listing gets its own row
    df_exploded = df_parsed.select(
        explode(col("parsed_data")).alias("listing"),
        col("scraped_at")
    )

    # 4. Flatten the struct into top-level columns
    # FIX: now selects currency_raw, sqft, property_type which were previously
    # absent here (and also missing from listing_schema), causing silent data loss.
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
        col("scraped_at").alias("extracted_at"),
    ).withColumn("spark_processed_at", current_timestamp())

    # 5. Write to PostgreSQL Staging Schema
    postgres_url = f"jdbc:postgresql://postgres:5432/{os.getenv('POSTGRES_DB')}"
    postgres_properties = {
        "user":     os.getenv('POSTGRES_USER'),
        "password": os.getenv('POSTGRES_PASSWORD'),
        "driver":   "org.postgresql.Driver",
    }

    df_flattened.write.jdbc(
        url=postgres_url,
        table="staging.stg_raw_listings",
        mode="append",
        properties=postgres_properties,
    )

    spark.stop()


if __name__ == "__main__":
    run_etl()