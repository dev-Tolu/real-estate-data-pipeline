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

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv('SCRAPER_LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry
# Maps a URL prefix to the parse strategy to use for that site.
# Add new sites here without touching any other code.
# ---------------------------------------------------------------------------
SOURCE_REGISTRY = {
    'propertypro.ng': 'propertypro',
    'nigeriapropertycentre.com': 'nigeriapropertycentre',
    'privateproperty.com.ng': 'privateproperty',
}

# Default paginated search URLs for each supported source.
# Override via DATA_SOURCE_URLS in .env if you want custom filters.
DEFAULT_URLS = [
    'https://www.propertypro.ng/properties/for-sale',
    'https://www.nigeriapropertycentre.com/for-sale',
    'https://privateproperty.ng/property-for-sale', 
]


class RealEstateScraper:
    def __init__(self):
        # MinIO configuration (data lake)
        self.minio_client = Minio(
            os.getenv('MINIO_ENDPOINT', 'minio:9000'),
            access_key=os.getenv('MINIO_ROOT_USER'),
            secret_key=os.getenv('MINIO_ROOT_PASSWORD'),
            secure=False
        )
        self.bucket_raw = os.getenv('S3_BUCKET_RAW', 'realestate-raw-data')

        # PostgreSQL configuration
        self.pg_conn = psycopg2.connect(dsn=os.getenv('POSTGRES_DSN'))

        # Redis configuration (caching + rate limiting)
        self.redis_client = Redis(
            host='redis',
            port=6379,
            password=os.getenv('REDIS_PASSWORD'),
            db=0,
            decode_responses=True
        )

        # Source URLs — fall back to defaults if env var is empty/missing
        env_urls = os.getenv('DATA_SOURCE_URLS', '').strip()
        self.source_urls = [u.strip() for u in env_urls.split(',') if u.strip()] or DEFAULT_URLS

        # Rate limiting (requests per second)
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
        })

    # ------------------------------------------------------------------
    # Fetching
    # ------------------------------------------------------------------

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def fetch_page(self, url: str) -> str:
        """Fetch a single page with rate limiting and retry."""
        elapsed = time.time() - self.last_request_time
        gap = 1.0 / self.rate_limit
        if elapsed < gap:
            time.sleep(gap - elapsed)

        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        self.last_request_time = time.time()
        return response.text

    def fetch_listings(self, url: str) -> List[Dict]:
        """Fetch and parse listings from a URL, following pagination up to 5 pages."""
        try:
            strategy = self._detect_strategy(url)
            all_listings: List[Dict] = []

            for page in range(1, 6):  # max 5 pages per run
                page_url = self._paginate_url(url, strategy, page)
                logger.info(f"Fetching page {page}: {page_url}")

                try:
                    html = self.fetch_page(page_url)
                except requests.RequestException as e:
                    logger.error(f"Failed to fetch {page_url}: {e}")
                    break

                listings = self._parse(html, url, strategy)
                if not listings:
                    logger.info(f"No listings on page {page}, stopping pagination")
                    break

                all_listings.extend(listings)
                logger.info(f"Page {page}: found {len(listings)} listings")

            return all_listings

        except Exception as e:
            logger.error(f"Failed to scrape {url}: {e}")
            raise

    # ------------------------------------------------------------------
    # Strategy routing
    # ------------------------------------------------------------------

    def _detect_strategy(self, url: str) -> str:
        for domain, strategy in SOURCE_REGISTRY.items():
            if domain in url:
                return strategy
        logger.warning(f"No strategy found for {url}, using generic parser")
        return 'generic'

    def _paginate_url(self, base_url: str, strategy: str, page: int) -> str:
        if page == 1:
            return base_url
        paginators = {
            'propertypro': f"{base_url}?page={page}",
            'nigeriapropertycentre': f"{base_url}?page={page}",
            'privateproperty': f"{base_url}?page={page}",
            'generic': f"{base_url}?page={page}",
        }
        return paginators.get(strategy, f"{base_url}?page={page}")

    def _parse(self, html: str, source_url: str, strategy: str) -> List[Dict]:
        parsers = {
            'propertypro': self._parse_propertypro,
            'nigeriapropertycentre': self._parse_nigeriapropertycentre,
            'privateproperty': self._parse_privateproperty,
            'generic': self._parse_generic,
        }
        parser = parsers.get(strategy, self._parse_generic)
        return parser(html, source_url)

    # ------------------------------------------------------------------
    # Site-specific parsers
    # ------------------------------------------------------------------

    def _parse_propertypro(self, html: str, source_url: str) -> List[Dict]:
        """UPDATED parser for propertypro.ng based on actual HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        listings = []
        
        # PropertyPro uses property-listing or property-listing-grid containers
        cards = soup.select('div.property-listing, div.property-listing-grid')
        
        logger.info(f"Found {len(cards)} listing cards on PropertyPro")
        
        for card in cards:
            try:
                # Title is in .pl-title h3 a
                title_el = card.select_one('.pl-title h3 a, .property-listing-content h3 a, h3 a')
                
                # Price in .pl-price h3
                price_el = card.select_one('.pl-price h3, [class*="price"] h3')
                price_text = self._text(price_el)
                
                # Clean price
                clean_price = None
                if price_text:
                    import re
                    price_match = re.search(r'([\d,]+(?:\.\d+)?)', price_text.replace('₦', '').replace('$', ''))
                    if price_match:
                        clean_price = float(price_match.group(1).replace(',', ''))
                
                # Location in .pl-title p
                location_el = card.select_one('.pl-title p, .property-listing-content address, [class*="location"]')
                
                # Extract bedrooms and bathrooms from .pl-price h6 (e.g., "5 Beds 5 Baths")
                details_el = card.select_one('.pl-price h6, .property-listing-content h6')
                beds = None
                baths = None
                
                if details_el:
                    details_text = self._text(details_el)
                    import re
                    beds_match = re.search(r'(\d+)\s*Bed', details_text, re.IGNORECASE)
                    baths_match = re.search(r'(\d+)\s*Bath', details_text, re.IGNORECASE)
                    beds = int(beds_match.group(1)) if beds_match else None
                    baths = int(baths_match.group(1)) if baths_match else None
                
                # Get listing URL
                link_el = card.select_one('.pl-title h3 a, .property-listing-content h3 a, a[href*="/property/"]')
                
                listing_url = None
                if link_el:
                    href = link_el.get('href', '')
                    if href.startswith('/'):
                        listing_url = f"https://propertypro.ng{href}"
                    elif href.startswith('http'):
                        listing_url = href
                
                # Extract property type from URL or card
                property_type = 'unknown'
                if listing_url:
                    if '/flat-apartment/' in listing_url or 'flat' in listing_url.lower():
                        property_type = 'flat/apartment'
                    elif '/house/' in listing_url or 'house' in listing_url.lower():
                        property_type = 'house'
                    elif '/land/' in listing_url:
                        property_type = 'land'
                    elif '/commercial-property/' in listing_url:
                        property_type = 'commercial'
                
                listing = {
                    'source_url': source_url,
                    'listing_url': listing_url,
                    'title': self._text(title_el),
                    'price': clean_price,
                    'location': self._text(location_el),
                    'address': self._text(location_el),
                    'bedrooms': beds,
                    'bathrooms': baths,
                    'sqft': None,
                    'property_type': property_type,
                    'listing_date': datetime.now().isoformat(),
                    'scraped_at': datetime.now().isoformat(),
                }
                
                if listing['title'] or listing['price']:
                    listings.append(listing)
                    logger.debug(f"Parsed: {listing['title'][:50] if listing['title'] else 'No title'} - ₦{listing['price']}")
                    
            except Exception as e:
                logger.warning(f"[propertypro] Failed to parse card: {e}")
                continue
        
        logger.info(f"PropertyPro: Successfully parsed {len(listings)} listings")
        return listings

    def _parse_nigeriapropertycentre(self, html: str, source_url: str) -> List[Dict]:
        """UPDATED parser for nigeriapropertycentre.com based on actual HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        listings = []
        
        # The actual listing containers - based on HTML structure
        cards = soup.select('div.row.property-list, div.wp-block.property.list')
        
        logger.info(f"Found {len(cards)} listing cards on NigeriaPropertyCentre")
        
        for card in cards:
            try:
                # Extract title from h3 or h4
                title_el = card.select_one('h3, h4, .content-title, [itemprop="name"]')
                
                # Extract price - looking for span with class "price"
                price_el = card.select_one('span.price, [class*="price"]')
                price_text = self._text(price_el)
                
                # Clean price
                clean_price = None
                if price_text:
                    import re
                    price_match = re.search(r'([\d,]+(?:\.\d+)?)', price_text.replace('₦', '').replace('$', ''))
                    if price_match:
                        clean_price = float(price_match.group(1).replace(',', ''))
                
                # Location is in address tag
                location_el = card.select_one('address, [class*="location"], [class*="address"]')
                
                # Extract bedrooms and bathrooms from aux-info
                beds = None
                baths = None
                
                # Look for bed icon
                beds_li = card.select_one('li i.fa-bed, li i.fal.fa-bed')
                if beds_li:
                    beds_li_parent = beds_li.find_parent('li')
                    if beds_li_parent:
                        beds_span = beds_li_parent.select_one('span')
                        if beds_span:
                            beds_text = beds_span.get_text(strip=True)
                            beds = self._parse_int(beds_text)
                
                # Look for bath icon
                baths_li = card.select_one('li i.fa-bath, li i.fal.fa-bath')
                if baths_li:
                    baths_li_parent = baths_li.find_parent('li')
                    if baths_li_parent:
                        baths_span = baths_li_parent.select_one('span')
                        if baths_span:
                            baths_text = baths_span.get_text(strip=True)
                            baths = self._parse_int(baths_text)
                
                # Get listing URL
                link_el = card.select_one('a[href*="/for-sale/"], a[href*="/property/"]')
                
                listing_url = None
                if link_el:
                    href = link_el.get('href', '')
                    if href.startswith('/'):
                        listing_url = f"https://www.nigeriapropertycentre.com{href}"
                    elif href.startswith('http'):
                        listing_url = href
                
                # Extract property type from URL or breadcrumb
                property_type = 'unknown'
                if listing_url:
                    if 'flats-apartments' in listing_url or 'flat' in listing_url:
                        property_type = 'flat/apartment'
                    elif 'houses' in listing_url or 'house' in listing_url:
                        property_type = 'house'
                    elif 'land' in listing_url:
                        property_type = 'land'
                    elif 'commercial' in listing_url:
                        property_type = 'commercial'
                
                listing = {
                    'source_url': source_url,
                    'listing_url': listing_url,
                    'title': self._text(title_el),
                    'price': clean_price,
                    'location': self._text(location_el),
                    'address': self._text(location_el),
                    'bedrooms': beds,
                    'bathrooms': baths,
                    'sqft': None,
                    'property_type': property_type,
                    'listing_date': datetime.now().isoformat(),
                    'scraped_at': datetime.now().isoformat(),
                }
                
                if listing['title'] or listing['price']:
                    listings.append(listing)
                    logger.debug(f"Parsed: {listing['title'][:50] if listing['title'] else 'No title'} - ₦{listing['price']}")
                    
            except Exception as e:
                logger.warning(f"[nigeriapropertycentre] Failed to parse card: {e}")
                continue
        
        logger.info(f"NigeriaPropertyCentre: Successfully parsed {len(listings)} listings")
        return listings

    def _parse_privateproperty(self, html: str, source_url: str) -> List[Dict]:
        """UPDATED parser for privateproperty.ng based on actual HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        listings = []
        
        # PrivateProperty uses similar-listings-item containers
        cards = soup.select('div.similar-listings-item, div.result-listings > div')
        
        logger.info(f"Found {len(cards)} listing cards on PrivateProperty")
        
        for card in cards:
            try:
                # Title is in h2 a within similar-listings-info
                title_el = card.select_one('.similar-listings-info h2 a, .similar-listings-info h2')
                
                # Property type is in h3
                type_el = card.select_one('.similar-listings-info h3')
                property_type = self._text(type_el) if type_el else 'unknown'
                
                # Price is in .similar-listings-price h4
                price_el = card.select_one('.similar-listings-price h4')
                price_text = self._text(price_el)
                
                # Location is in .listings-location
                location_el = card.select_one('.listings-location')
                
                # Bedrooms and bathrooms from property-benefit icons
                # Look for li elements with icons that indicate counts
                benefit_items = card.select('.property-benefit li')
                beds = None
                baths = None
                
                for item in benefit_items:
                    # Check for bed icon (fa-bed)
                    if item.select_one('svg path') or item.select_one('i'):
                        # Look for text content
                        item_text = self._text(item)
                        # Some items have numbers, some don't
                        if item_text and item_text.isdigit():
                            if beds is None:
                                beds = int(item_text)
                            else:
                                baths = int(item_text)
                
                # Alternative: look for the text pattern in the price area
                price_details = card.select_one('.similar-listings-price h4')
                if price_details and not beds:
                    # Sometimes bed info is in the price area
                    parent_text = price_details.find_parent().get_text() if price_details.find_parent() else ''
                    import re
                    beds_match = re.search(r'(\d+)\s*bed', parent_text.lower())
                    if beds_match:
                        beds = int(beds_match.group(1))
                
                # Get listing URL
                link_el = card.select_one('.similar-listings-info h2 a, a[href*="/listings/"]')
                
                listing_url = None
                if link_el:
                    href = link_el.get('href', '')
                    if href.startswith('/'):
                        listing_url = f"https://privateproperty.ng{href}"
                    elif href.startswith('http'):
                        listing_url = href
                
                # Extract agent/company name
                agent_el = card.select_one('.media .media-body h5, .media-body h5')
                agent_name = self._text(agent_el) if agent_el else None
                
                # Extract update date
                date_el = card.select_one('.media-body h5, .date-added')
                date_text = self._text(date_el) if date_el else None
                
                # Clean price - remove currency symbol and convert
                clean_price = None
                if price_text:
                    # Remove ₦ and $ symbols, commas, and convert to float
                    import re
                    price_match = re.search(r'([\d,]+(?:\.\d+)?)', price_text.replace('₦', '').replace('$', ''))
                    if price_match:
                        clean_price = float(price_match.group(1).replace(',', ''))
                
                listing = {
                    'source_url': source_url,
                    'listing_url': listing_url,
                    'title': self._text(title_el),
                    'price': clean_price,
                    'location': self._text(location_el),
                    'address': self._text(location_el),
                    'bedrooms': beds,
                    'bathrooms': baths,
                    'sqft': None,
                    'property_type': property_type.lower() if property_type else 'unknown',
                    'agent_name': agent_name,
                    'listing_date': datetime.now().isoformat(),
                    'scraped_at': datetime.now().isoformat(),
                }
                
                # Only add if we have meaningful data
                if listing['title'] or listing['price']:
                    listings.append(listing)
                    logger.debug(f"Parsed: {listing['title'][:50] if listing['title'] else 'No title'} - ₦{listing['price']}")
                    
            except Exception as e:
                logger.warning(f"[privateproperty] Failed to parse card: {e}")
                continue
        
        logger.info(f"PrivateProperty: Successfully parsed {len(listings)} listings")
        return listings

    def _parse_generic(self, html: str, source_url: str) -> List[Dict]:
        """Fallback parser — tries common real-estate CSS patterns."""
        soup = BeautifulSoup(html, 'html.parser')
        listings = []

        cards = soup.select(
            '.listing, .property-card, .property-item, '
            'article.property, div[class*="listing"], div[class*="property"]'
        )
        for card in cards:
            try:
                listing = {
                    'source_url': source_url,
                    'listing_url': None,
                    'title': self._text(card.select_one('h1,h2,h3,h4')),
                    'price': self._parse_price(self._text(card.select_one('.price,[class*="price"]'))),
                    'location': self._text(card.select_one('.location,.address,[class*="location"]')),
                    'address': self._text(card.select_one('.address,[data-address]')),
                    'bedrooms': self._parse_int(self._text(card.select_one('.beds,[class*="bed"]'))),
                    'bathrooms': self._parse_float(self._text(card.select_one('.baths,[class*="bath"]'))),
                    'sqft': self._parse_int(self._text(card.select_one('.sqft,[class*="area"]'))),
                    'property_type': self._text(card.select_one('.type,.tag,[class*="type"]')) or 'unknown',
                    'listing_date': datetime.now().isoformat(),
                    'scraped_at': datetime.now().isoformat(),
                }
                listings.append(listing)
            except Exception as e:
                logger.warning(f"[generic] Failed to parse card: {e}")

        return listings
    
    

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _text(el) -> Optional[str]:
        return el.get_text(strip=True) if el else None

    @staticmethod
    def _abs_url(base: str, el) -> Optional[str]:
        if not el:
            return None
        href = el.get('href', '')
        if href.startswith('http'):
            return href
        return base.rstrip('/') + '/' + href.lstrip('/')

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
    def _parse_int(text: Optional[str]) -> Optional[int]:
        if not text:
            return None
        digits = ''.join(c for c in text if c.isdigit())
        return int(digits) if digits else None

    @staticmethod
    def _parse_float(text: Optional[str]) -> Optional[float]:
        if not text:
            return None
        digits = ''.join(c for c in text if c.isdigit() or c == '.')
        try:
            return float(digits) if digits else None
        except ValueError:
            return None

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save_to_minio(self, data: List[Dict], source_url: str) -> str:
        """Save raw data to MinIO (data lake)."""
        try:
            if not self.minio_client.bucket_exists(self.bucket_raw):
                self.minio_client.make_bucket(self.bucket_raw)

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            source_name = source_url.split('//')[-1].replace('/', '_')[:50]
            filename = f"{timestamp}_{source_name}.json"

            data_str = json.dumps(data, indent=2)
            data_bytes = data_str.encode('utf-8')
            self.minio_client.put_object(
                self.bucket_raw,
                filename,
                data=__import__('io').BytesIO(data_bytes),
                length=len(data_bytes),
                content_type='application/json'
            )
            logger.info(f"Saved {len(data)} records to MinIO: {filename}")
            return filename

        except S3Error as e:
            logger.error(f"Failed to save to MinIO: {e}")
            raise

    def save_to_postgres(self, data: List[Dict]):
        """Save parsed data to PostgreSQL."""
        if not data:
            logger.warning("No data to save to PostgreSQL")
            return

        cursor = self.pg_conn.cursor()

        property_query = """
            INSERT INTO properties (
                property_id, address, city, state, zip_code,
                property_type, bedrooms, bathrooms, living_area_sqft, created_at
            ) VALUES %s
            ON CONFLICT (property_id) DO UPDATE SET
                address = EXCLUDED.address,
                property_type = EXCLUDED.property_type,
                bedrooms = EXCLUDED.bedrooms,
                bathrooms = EXCLUDED.bathrooms,
                living_area_sqft = EXCLUDED.living_area_sqft,
                updated_at = CURRENT_TIMESTAMP
        """

        property_values = []
        for item in data:
            property_id = (
                f"{item['source_url']}_{item.get('address') or item.get('location') or item.get('title') or ''}"
            ).replace(' ', '_').lower()[:255]

            property_values.append((
                property_id,
                item.get('address') or item.get('location'),
                None,   # city  — parse from address in ETL layer
                None,   # state
                None,   # zip_code
                item.get('property_type'),
                item.get('bedrooms'),
                item.get('bathrooms'),
                item.get('sqft'),
                datetime.now(),
            ))

        if property_values:
            execute_values(cursor, property_query, property_values)

        price_query = """
            INSERT INTO price_history (
                property_id, price, listing_date, sale_date,
                price_per_sqft, status, created_at
            ) VALUES %s
            ON CONFLICT (property_id, listing_date) DO NOTHING
        """

        price_values = []
        for item in data:
            property_id = (
                f"{item['source_url']}_{item.get('address') or item.get('location') or item.get('title') or ''}"
            ).replace(' ', '_').lower()[:255]

            price = item.get('price')
            sqft = item.get('sqft')
            price_values.append((
                property_id,
                price,
                datetime.now().date(),
                None,
                round(price / sqft, 2) if price and sqft else None,
                'active',
                datetime.now(),
            ))

        if price_values:
            execute_values(cursor, price_query, price_values)

        cursor.execute("""
            INSERT INTO scraper_logs (
                job_id, source_url, records_scraped, status,
                started_at, completed_at, duration_seconds
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            f"scrape_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            ','.join(self.source_urls),
            len(data),
            'success',
            datetime.now(),
            datetime.now(),
            0,
        ))

        self.pg_conn.commit()
        cursor.close()
        logger.info(f"Saved {len(data)} records to PostgreSQL")

    def cache_in_redis(self, key: str, data: List[Dict], ttl: int = 3600):
        """Cache results in Redis for fast access."""
        self.redis_client.setex(
            f"scraper:{key}",
            ttl,
            json.dumps(data)
        )
        logger.debug(f"Cached {len(data)} records in Redis")

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def run(self):
        """Main scraping orchestration."""
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
                    logger.info(f"Successfully scraped {len(listings)} listings from {url}")
                else:
                    logger.warning(f"No listings found from {url} — selectors may need updating")

            except Exception as e:
                logger.error(f"Failed to scrape {url}: {e}")
                continue

        duration = (datetime.now() - job_start).seconds
        logger.info(f"Scraping complete. Total listings: {len(all_listings)}. Duration: {duration}s")
        return all_listings


if __name__ == "__main__":
    scraper = RealEstateScraper()
    scraper.run()