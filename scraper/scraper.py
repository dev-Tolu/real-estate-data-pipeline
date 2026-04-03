import io
import os
import json
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from minio import Minio
from minio.error import S3Error
import psycopg2
from psycopg2.extras import execute_values
from redis import Redis
import time
from typing import List, Dict, Optional
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(
    level=getattr(logging, os.getenv('SCRAPER_LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Supported sources
# propertypro.ng and privateproperty.com.ng return 403 — they block scrapers.
# nigeriapropertycentre.com is openly accessible and well-structured.
# ---------------------------------------------------------------------------
DEFAULT_URLS = [
    'https://nigeriapropertycentre.com/for-sale',
    'https://nigeriapropertycentre.com/for-sale/lagos',
    'https://nigeriapropertycentre.com/for-sale/abuja',
]

SOURCE_REGISTRY = {
    'nigeriapropertycentre.com': 'nigeriapropertycentre',
}


class RealEstateScraper:
    def __init__(self):
        self.minio_client = Minio(
            os.getenv('MINIO_ENDPOINT', 'minio:9000'),
            access_key=os.getenv('MINIO_ROOT_USER'),
            secret_key=os.getenv('MINIO_ROOT_PASSWORD'),
            secure=False
        )
        self.bucket_raw = os.getenv('S3_BUCKET_RAW', 'realestate-raw-data')

        self.pg_conn = psycopg2.connect(dsn=os.getenv('POSTGRES_DSN'))

        self.redis_client = Redis(
            host='redis',
            port=6379,
            password=os.getenv('REDIS_PASSWORD'),
            db=0,
            decode_responses=True
        )

        env_urls = os.getenv('DATA_SOURCE_URLS', '').strip()
        self.source_urls = (
            [u.strip() for u in env_urls.split(',') if u.strip()]
            or DEFAULT_URLS
        )

        self.rate_limit = int(os.getenv('SCRAPER_RATE_LIMIT', 2))
        self.last_request_time = 0

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/120.0.0.0 Safari/537.36'
            ),
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Referer': 'https://nigeriapropertycentre.com/',
        })

    # ------------------------------------------------------------------
    # Fetching
    # ------------------------------------------------------------------

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def fetch_page(self, url: str) -> str:
        elapsed = time.time() - self.last_request_time
        gap = 1.0 / self.rate_limit
        if elapsed < gap:
            time.sleep(gap - elapsed)
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        self.last_request_time = time.time()
        return response.text

    def fetch_listings(self, url: str) -> List[Dict]:
        strategy = self._detect_strategy(url)
        if strategy is None:
            logger.warning(
                f"Skipping {url} — site blocks scrapers (403). "
                f"Only nigeriapropertycentre.com is currently supported."
            )
            return []

        all_listings: List[Dict] = []
        for page in range(1, 6):
            page_url = self._paginate(url, page)
            logger.info(f"Fetching page {page}: {page_url}")
            try:
                html = self.fetch_page(page_url)
            except requests.RequestException as e:
                logger.error(f"Failed to fetch {page_url}: {e}")
                break

            listings = self._parse_nigeriapropertycentre(html, url)
            if not listings:
                logger.info(f"No listings on page {page}, stopping pagination")
                break

            all_listings.extend(listings)
            logger.info(f"Page {page}: found {len(listings)} listings")

        return all_listings

    def _detect_strategy(self, url: str) -> Optional[str]:
        for domain, strategy in SOURCE_REGISTRY.items():
            if domain in url:
                return strategy
        return None

    @staticmethod
    def _paginate(base_url: str, page: int) -> str:
        if page == 1:
            return base_url
        sep = '&' if '?' in base_url else '?'
        return f"{base_url}{sep}page={page}"

    # ------------------------------------------------------------------
    # Parser — nigeriapropertycentre.com
    #
    # Verified from live HTML (2026-04):
    #   Cards: <div class="listings-property"> or <li class="listings-property">
    #   Title: h3 > a  or  h4 > a
    #   Price: element containing ₦ symbol
    #   Location: <address> tag or [class*="location"]
    #   Features: <li> bullets containing "Bedroom", "Bathroom" etc.
    # ------------------------------------------------------------------

    def _parse_nigeriapropertycentre(self, html: str, source_url: str):
        soup = BeautifulSoup(html, "html.parser")
        listings = []

        cards = soup.find_all("h4")

        logger.info(f"Found {len(cards)} h4 listing blocks")

        for h in cards:
            try:
                link = h.find("a")
                if not link:
                    continue

                title = link.get_text(strip=True)
                href = link.get("href")

                listing_url = (
                    href if href.startswith("http")
                    else f"https://nigeriapropertycentre.com{href}"
                )

                # Look for price in nearby elements (siblings)
                price = None
                node = h

                for _ in range(10):  # walk forward safely
                    node = node.next_sibling
                    if not node:
                        break

                    text = str(node)
                    if "₦" in text:
                        price = text.strip()
                        break

                # Extract features (bedrooms, bathrooms)
                bedrooms = None
                bathrooms = None

                parent_text = h.parent.get_text(" ", strip=True).lower()

                import re
                bed_match = re.search(r"(\d+)\s*bed", parent_text)
                bath_match = re.search(r"(\d+)\s*bath", parent_text)

                if bed_match:
                    bedrooms = int(bed_match.group(1))
                if bath_match:
                    bathrooms = int(bath_match.group(1))

                listing = {
                    "source_url": source_url,
                    "listing_url": listing_url,
                    "title": title,
                    "price": self._parse_price(price),
                    "location": None,
                    "address": None,
                    "bedrooms": bedrooms,
                    "bathrooms": bathrooms,
                    "sqft": None,
                    "property_type": self._infer_type(title),
                    "listing_date": datetime.now().isoformat(),
                    "scraped_at": datetime.now().isoformat(),
                }

                listings.append(listing)

            except Exception as e:
                logger.warning(f"Error parsing listing: {e}")

        logger.info(f"Parsed {len(listings)} listings")
        return listings

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _text(el) -> Optional[str]:
        if el is None:
            return None
        if hasattr(el, 'get_text'):
            return el.get_text(separator=' ', strip=True)
        return str(el).strip()

    @staticmethod
    def _parse_price(text: Optional[str]) -> Optional[float]:
        if not text:
            return None
        digits = ''.join(c for c in text if c.isdigit() or c == '.')
        try:
            return float(digits) if digits else None
        except ValueError:
            return None

    @staticmethod
    def _extract_feature(card, keyword: str) -> Optional[int]:
        for el in card.select('li, span, td'):
            text = el.get_text(strip=True)
            if keyword.lower() in text.lower():
                digits = ''.join(c for c in text if c.isdigit())
                return int(digits) if digits else None
        return None

    @staticmethod
    def _infer_type(title: str) -> str:
        t = title.lower()
        if any(w in t for w in ['flat', 'apartment']): return 'flat/apartment'
        if 'duplex' in t:                               return 'duplex'
        if any(w in t for w in ['house', 'bungalow', 'villa', 'detached', 'mansion']): return 'house'
        if 'land' in t:                                 return 'land'
        if any(w in t for w in ['office', 'shop', 'commercial', 'warehouse']): return 'commercial'
        return 'unknown'

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save_to_minio(self, data: List[Dict], source_url: str) -> str:
        try:
            if not self.minio_client.bucket_exists(self.bucket_raw):
                self.minio_client.make_bucket(self.bucket_raw)

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            source_name = source_url.split('//')[-1].replace('/', '_')[:50]
            filename = f"{timestamp}_{source_name}.json"

            data_bytes = json.dumps(data, indent=2).encode('utf-8')
            self.minio_client.put_object(
                self.bucket_raw,
                filename,
                data=io.BytesIO(data_bytes),
                length=len(data_bytes),
                content_type='application/json'
            )
            logger.info(f"Saved {len(data)} records to MinIO: {filename}")
            return filename
        except S3Error as e:
            logger.error(f"Failed to save to MinIO: {e}")
            raise

    def save_to_postgres(self, data: List[Dict]):
        if not data:
            return
        cursor = self.pg_conn.cursor()

        def make_pid(item):
            return (
                f"{item['source_url']}_{item.get('address') or item.get('title') or ''}"
            ).replace(' ', '_').lower()[:255]

        execute_values(cursor, """
            INSERT INTO properties (
                property_id, address, city, state, zip_code,
                property_type, bedrooms, bathrooms, living_area_sqft, created_at
            ) VALUES %s
            ON CONFLICT (property_id) DO UPDATE SET
                address        = EXCLUDED.address,
                property_type  = EXCLUDED.property_type,
                bedrooms       = EXCLUDED.bedrooms,
                bathrooms      = EXCLUDED.bathrooms,
                living_area_sqft = EXCLUDED.living_area_sqft,
                updated_at     = CURRENT_TIMESTAMP
        """, [(
            make_pid(i), i.get('address') or i.get('location'),
            None, None, None,
            i.get('property_type'), i.get('bedrooms'), i.get('bathrooms'),
            i.get('sqft'), datetime.now(),
        ) for i in data])

        execute_values(cursor, """
            INSERT INTO price_history (
                property_id, price, listing_date, sale_date,
                price_per_sqft, status, created_at
            ) VALUES %s
            ON CONFLICT (property_id, listing_date) DO NOTHING
        """, [(
            make_pid(i), i.get('price'), datetime.now().date(), None,
            round(i['price'] / i['sqft'], 2) if i.get('price') and i.get('sqft') else None,
            'active', datetime.now(),
        ) for i in data])

        cursor.execute("""
            INSERT INTO scraper_logs (
                job_id, source_url, records_scraped, status,
                started_at, completed_at, duration_seconds
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            f"scrape_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            ','.join(self.source_urls), len(data), 'success',
            datetime.now(), datetime.now(), 0,
        ))

        self.pg_conn.commit()
        cursor.close()
        logger.info(f"Saved {len(data)} records to PostgreSQL")

    def cache_in_redis(self, key: str, data: List[Dict], ttl: int = 3600):
        self.redis_client.setex(f"scraper:{key}", ttl, json.dumps(data))
        logger.debug(f"Cached {len(data)} records in Redis")

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def run(self):
        all_listings = []
        job_start = datetime.now()

        for url in self.source_urls:
            if not url:
                continue
            try:
                logger.info(f"Scraping {url}")
                listings = self.fetch_listings(url)
                if listings:
                    self.save_to_minio(listings, url)
                    self.save_to_postgres(listings)
                    self.cache_in_redis(url, listings)
                    all_listings.extend(listings)
                    logger.info(f"Scraped {len(listings)} listings from {url}")
                else:
                    logger.warning(
                        f"No listings found from {url} — "
                        f"set SCRAPER_LOG_LEVEL=DEBUG to see raw card count"
                    )
            except Exception as e:
                logger.error(f"Failed to scrape {url}: {e}")

        duration = (datetime.now() - job_start).seconds
        logger.info(f"Done. Total: {len(all_listings)} listings in {duration}s")
        return all_listings


if __name__ == "__main__":
    scraper = RealEstateScraper()
    scraper.run()