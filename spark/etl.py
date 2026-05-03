#!/usr/bin/env python3
"""
Bronze → Silver ETL for the Real Estate Data Pipeline.

This script reads raw HTML pages from MinIO (Bronze), parses them to extract
structured listing data, and writes the cleaned records to a PostgreSQL staging
table (Silver) for further processing.
"""
import os
import hashlib
import re
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode, current_timestamp
from pyspark.sql.types import (
    ArrayType, StringType, StructType, StructField, IntegerType
)
from bs4 import BeautifulSoup

# ---------------------------------------------------------
# 1. Schema for raw Bronze records (what MinIO JSON contains)
# ---------------------------------------------------------
raw_schema = StructType([
    StructField("source_url", StringType(), True),
    StructField("html",       StringType(), True),
    StructField("scraped_at", StringType(), True),
])

# ---------------------------------------------------------
# 2. Schema for each parsed listing struct returned by the UDF
# ---------------------------------------------------------
listing_schema = StructType([
    StructField("property_id",   StringType(),  True),
    StructField("url",           StringType(),  True),
    StructField("title",         StringType(),  True),
    StructField("price_raw",     StringType(),  True),
    StructField("currency_raw",  StringType(),  True),
    StructField("location",      StringType(),  True),
    StructField("bedrooms",      IntegerType(), True),
    StructField("bathrooms",     IntegerType(), True),
    StructField("sqft",          StringType(),  True),   # kept as String; cast later
    StructField("property_type", StringType(),  True),
    StructField("source_site",   StringType(),  True),
])


# ---------------------------------------------------------
# 3. HTML Parser UDF
# ---------------------------------------------------------
def parse_html_to_listings(html: str, source_url: str):
    if not html:
        return []

    def _text(el):
        return el.get_text(strip=True) if el else None

    def _parse_int(text):
        """Return Python int or None — must match IntegerType() in listing_schema."""
        if not text:
            return None
        digits = "".join(c for c in str(text) if c.isdigit())
        return int(digits) if digits else None

    def extract_type_from_url(url_string):
        if not url_string:
            return None
        u = url_string.lower()
        if "block-of-flats"       in u: return "Block Of Flats"
        if "mini-flat"            in u: return "Mini Flat"
        if "self-contain"         in u: return "Self Contain"
        if "shared-apartment"     in u: return "Shared Apartment"
        if "studio-apartment"     in u: return "Studio Apartment"
        if "penthouse"            in u: return "Penthouse"
        if "rooms-and-boys-quarter" in u or "boys-quarter" in u:
            return "Rooms & Boys Quarters"
        if "flat" in u or "apartment" in u: return "Flats & Apartments"
        if "semi-detached-duplex" in u: return "Semi-Detached Duplex"
        if "detached-duplex"      in u: return "Detached Duplex"
        if "terraced-duplex"      in u: return "Terraced Duplex"
        if "terrace"              in u: return "Terraced Duplex"
        if "duplex"               in u: return "Duplex"
        if "semi-detached-bungalow" in u: return "Semi-Detached Bungalow"
        if "detached-bungalow"    in u: return "Detached Bungalow"
        if "terraced-bungalow"    in u: return "Terraced Bungalow"
        if "bungalow"             in u: return "Bungalow"
        if "townhouse"            in u: return "Townhouse"
        if "maisonette"           in u: return "Maisonette"
        if "house"                in u: return "House"
        if "commercial-land"      in u: return "Commercial Land"
        if "industrial-land"      in u: return "Industrial Land"
        if "joint-venture-land"   in u: return "Joint Venture Land"
        if "mixed-use-land"       in u: return "Mixed-use Land"
        if "residential-land"     in u: return "Residential Land"
        if "land"  in u or "plot" in u: return "Land"
        if "co-working-space"     in u: return "Co-working Space"
        if "conference-room"      in u or "meeting-room" in u: return "Conference/Meeting Room"
        if "desk" in u or "workstation" in u: return "Desk/Workstation"
        if "event-hall"           in u: return "Event Hall"
        if "factory"              in u: return "Factory"
        if "filling-station"      in u or "gas-plant" in u or "tank-farm" in u:
            return "Oil & Gas Property"
        if "hostel"               in u: return "Hostel"
        if "hotel"                in u: return "Hotel"
        if "mall"                 in u: return "Mall/Complex/Plaza"
        if "office"               in u: return "Office"
        if "restaurant-bar"       in u: return "Restaurant/Bar"
        if "school"               in u: return "School"
        if "shop"                 in u: return "Shop"
        if "warehouse"            in u: return "Warehouse"
        if "commercial"           in u: return "Commercial Property"
        return None

    soup = BeautifulSoup(html, "html.parser")
    listings = []

    # ------------------------------------------------------------------
    # PropertyPro.ng
    # ------------------------------------------------------------------
    if "propertypro.ng" in source_url:
        source_site = "PropertyPro"
        # Only select the outer wrapper; the grid child is always nested
        # inside it, so a compound selector would double every listing.
        cards = soup.select("div.property-listing")
        for card in cards:
            try:
                title_el    = card.select_one(".pl-title h3 a, .property-listing-content h3 a, h3 a")
                price_el    = card.select_one(".pl-price h3, [class*='price'] h3")
                location_el = card.select_one(".pl-title p, .property-listing-content address, [class*='location']")
                details_el  = card.select_one(".pl-price h6, .property-listing-content h6")
                type_badge  = card.select_one(".property-features span, .pl-title h4")
                prop_type   = _text(type_badge)

                beds, baths = None, None
                if details_el:
                    dt = _text(details_el)
                    bm = re.search(r"(\d+)\s*Bed",  dt, re.IGNORECASE)
                    bam = re.search(r"(\d+)\s*Bath", dt, re.IGNORECASE)
                    beds  = _parse_int(bm.group(1))  if bm  else None
                    baths = _parse_int(bam.group(1)) if bam else None

                link_el = card.select_one(".pl-title h3 a, .property-listing-content h3 a, a[href*='/property/']")
                url = link_el.get("href", "") if link_el else ""
                if url and not url.startswith("http"):
                    url = f"https://propertypro.ng{url}"

                if not prop_type:
                    prop_type = extract_type_from_url(url)

                prop_id = hashlib.md5(f"PropertyPro_{url}_{_text(title_el)}".lower().encode()).hexdigest()[:32]

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

    # ------------------------------------------------------------------
    # PrivateProperty.ng
    # ------------------------------------------------------------------
    elif "privateproperty.ng" in source_url:
        source_site = "PrivateProperty"
        cards = soup.select("div.similar-listings-item")
        for card in cards:
            try:
                title_el    = card.select_one(".similar-listings-info h2 a, .similar-listings-info h2")
                price_el    = card.select_one(".similar-listings-price h4")
                location_el = card.select_one(".listings-location")
                type_el     = card.select_one(".listings-type, .property-type")
                prop_type   = _text(type_el)

                price_text = _text(price_el)
                currency   = "USD" if price_text and "$" in price_text else "NGN"

                beds, baths = None, None
                benefit_lis = card.select("ul.property-benefit li")
                if len(benefit_lis) >= 2:
                    beds  = _parse_int(_text(benefit_lis[0]))
                    baths = _parse_int(_text(benefit_lis[1]))
                else:
                    ft = card.get_text().lower()
                    bm = re.search(r"(\d+)\s*bed", ft)
                    if bm:
                        beds = _parse_int(bm.group(1))

                link_el = card.select_one(".similar-listings-info h2 a, a[href*='/listings/']")
                url = link_el.get("href", "") if link_el else ""
                if url and not url.startswith("http"):
                    url = f"https://privateproperty.ng{url}"

                if not prop_type:
                    prop_type = extract_type_from_url(url)

                prop_id = hashlib.md5(f"PrivateProperty_{url}_{_text(title_el)}".lower().encode()).hexdigest()[:32]

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

    # ------------------------------------------------------------------
    # NigeriaPropertyCentre.com
    # ------------------------------------------------------------------
    elif "nigeriapropertycentre.com" in source_url:
        source_site = "NigeriaPropertyCentre"
        # Use only the outer wrapper — the inner .wp-block.property.list
        # is always nested, so including both doubles every listing.
        cards = soup.select("div.row.property-list")
        for card in cards:
            try:
                title_el    = card.select_one("h3, h4, .content-title, [itemprop='name']")
                location_el = card.select_one("address, [class*='location'], [class*='address']")

                # Price is split across two sibling <span class="price"> elements
                # (one for the currency symbol, one for the number).
                price_spans = card.select("span.price")
                price_raw   = "".join(_text(s) or "" for s in price_spans).strip() or None

                beds, baths = None, None
                beds_li  = card.select_one("li i.fa-bed, li i.fal.fa-bed")
                baths_li = card.select_one("li i.fa-bath, li i.fal.fa-bath")
                if beds_li and beds_li.find_parent("li"):
                    beds  = _parse_int(_text(beds_li.find_parent("li").select_one("span")))
                if baths_li and baths_li.find_parent("li"):
                    baths = _parse_int(_text(baths_li.find_parent("li").select_one("span")))

                link_el = card.select_one("a[href*='/for-sale/'], a[href*='/property/']")
                url = link_el.get("href", "") if link_el else ""
                if url and not url.startswith("http"):
                    url = f"https://www.nigeriapropertycentre.com{url}"

                prop_type = extract_type_from_url(url)
                prop_id   = hashlib.md5(f"NigeriaPropertyCentre_{url}_{_text(title_el)}".lower().encode()).hexdigest()[:32]

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

    # ------------------------------------------------------------------
    # Generic fallback
    # ------------------------------------------------------------------
    else:
        source_site = "Unknown"
        cards = soup.select(
            ".listing, .property-card, .property-item, article.property, div[class*='listing']"
        )
        for card in cards:
            try:
                link_el = card.select_one("a")
                url = link_el.get("href", "") if link_el else ""
                if url and not url.startswith("http"):
                    url = source_url.rstrip("/") + "/" + url.lstrip("/")

                title_el = card.select_one("h1,h2,h3,h4")
                prop_id  = hashlib.md5(f"Unknown_{url}_{_text(title_el)}".lower().encode()).hexdigest()[:32]

                listings.append({
                    "property_id":   prop_id,
                    "url":           url,
                    "title":         _text(title_el),
                    "price_raw":     _text(card.select_one(".price,[class*='price']")),
                    "currency_raw":  "NGN",
                    "location":      _text(card.select_one(".location,.address,[class*='location']")),
                    "bedrooms":      _parse_int(_text(card.select_one(".beds,[class*='bed']"))),
                    "bathrooms":     _parse_int(_text(card.select_one(".baths,[class*='bath']"))),
                    "sqft":          _text(card.select_one(".sqft,[class*='area']")),
                    "property_type": _text(card.select_one(".type,.tag")) or "unknown",
                    "source_site":   source_site,
                })
            except Exception:
                continue

    return listings


parse_udf = udf(parse_html_to_listings, ArrayType(listing_schema))


# ---------------------------------------------------------
# 4. Main ETL entry point
# ---------------------------------------------------------
def run_etl():
    spark = (
        SparkSession.builder
        .appName("RealEstate_Bronze_to_Silver")
        .config("spark.hadoop.fs.s3a.endpoint",          f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}")
        .config("spark.hadoop.fs.s3a.access.key",        os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key",        os.getenv("MINIO_ROOT_PASSWORD"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )

    bucket = os.getenv("S3_BUCKET_RAW", "realestate-raw-data")

    # 1. Read raw JSON with explicit schema — no type inference surprises.
    df_raw = (
        spark.read
        .schema(raw_schema)
        .option("multiline", "true")
        .json(f"s3a://{bucket}/*.json")
    )

    # 2. Parse HTML via UDF — one array of listing structs per Bronze page.
    df_parsed = df_raw.withColumn("parsed_data", parse_udf(col("html"), col("source_url")))

    # 3. Explode → one row per listing.
    df_exploded = df_parsed.select(
        explode(col("parsed_data")).alias("listing"),
        col("scraped_at"),
    )

    # 4. Flatten struct → top-level columns matching staging.stg_raw_listings.
    #    sqft is stored as StringType in the UDF result (sites rarely provide a
    #    clean integer here) and cast to INTEGER for the Postgres column.
    df_flattened = (
        df_exploded.select(
            col("listing.property_id"),
            col("listing.url"),
            col("listing.title"),
            col("listing.price_raw"),
            col("listing.currency_raw"),
            col("listing.location"),
            col("listing.bedrooms"),
            col("listing.bathrooms"),
            col("listing.sqft").cast(IntegerType()).alias("sqft"),
            col("listing.property_type"),
            col("listing.source_site"),
            col("scraped_at").cast("timestamp").alias("extracted_at"),
        )
        .withColumn("spark_processed_at", current_timestamp())
    )

    # 5. Write to PostgreSQL staging table.
    postgres_url = f"jdbc:postgresql://postgres:5432/{os.getenv('POSTGRES_DB')}"
    postgres_props = {
        "user":     os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "driver":   "org.postgresql.Driver",
    }

    df_flattened.write.jdbc(
        url=postgres_url,
        table="staging.stg_raw_listings",
        mode="append",
        properties=postgres_props,
    )

    spark.stop()


if __name__ == "__main__":
    run_etl()
