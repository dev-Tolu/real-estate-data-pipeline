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

                # Extract description and features
                description = self._get_description_text(card, 'propertypro')
                features = self._extract_features_from_description(description)
                
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
                    'description': description,
                    'features': features,
                    'listing_date': datetime.now().isoformat(),
                    'scraped_at': datetime.now().isoformat(),
                }

                # Optional: Add individual feature fields for easier querying
                if features.get('swimming_pool'):
                    listing['has_pool'] = True
                if features.get('backup_power'):
                    listing['has_backup_power'] = True
                if features.get('parking_spaces'):
                    listing['parking_spaces'] = features['parking_spaces']
                
                listings.append(listing)
                
                if listing['title'] or listing['price']:
                    listings.append(listing)
                    logger.debug(f"Parsed: {listing['title'][:50] if listing['title'] else 'No title'} - ₦{listing['price']}")
                    
            except Exception as e:
                logger.warning(f"[propertypro] Failed to parse card: {e}")
                continue
        
        logger.info(f"PropertyPro: Successfully parsed {len(listings)} listings")
        return listings

    def _parse_nigeriapropertycentre(self, html: str, source_url: str) -> List[Dict]:
        """UPDATED parser for nigeriapropertycentre.com"""
        soup = BeautifulSoup(html, 'html.parser')
        listings = []
        
        cards = soup.select('div.row.property-list, div.wp-block.property.list')
        
        logger.info(f"Found {len(cards)} listing cards on NigeriaPropertyCentre")
        
        for card in cards:
            try:
                title_el = card.select_one('h3, h4, .content-title, [itemprop="name"]')
                
                price_el = None
                price_selectors = [
                    'span.price',
                    '[class*="price"]',
                    '.similar-listings-price h4',
                    '.pl-price h3',
                    'h4 span[content]'
                ]
                
                for selector in price_selectors:
                    price_el = card.select_one(selector)
                    if price_el:
                        break
                
                price_text = self._text(price_el) if price_el else None
                
                # Clean price - handle empty or invalid values
                clean_price = None
                if price_text and price_text != '₦':
                    import re
                    # Remove currency symbols and extract numbers
                    price_clean = re.sub(r'[^\d.,]', '', price_text)
                    if price_clean:
                        # Handle both comma and dot decimal separators
                        price_clean = price_clean.replace(',', '')
                        try:
                            clean_price = float(price_clean)
                        except ValueError:
                            clean_price = None
                
                # If price is still None, try to find it in the text content
                if not clean_price:
                    # Look for price in card text
                    card_text = card.get_text()
                    ngn_match = re.search(r'₦\s*([\d,]+)', card_text)
                    if ngn_match:
                        clean_price = float(ngn_match.group(1).replace(',', ''))
                    else:
                    # Look for any large number that might be a price
                        number_match = re.search(r'([\d,]{6,})', card_text)
                    if number_match:
                        clean_price = float(number_match.group(1).replace(',', ''))
                
                location_el = card.select_one('address, [class*="location"], [class*="address"]')
                
                # Extract bedrooms and bathrooms
                beds = None
                baths = None
                
                beds_li = card.select_one('li i.fa-bed, li i.fal.fa-bed')
                if beds_li:
                    beds_li_parent = beds_li.find_parent('li')
                    if beds_li_parent:
                        beds_span = beds_li_parent.select_one('span')
                        if beds_span:
                            beds_text = beds_span.get_text(strip=True)
                            beds = self._parse_int(beds_text)
                
                baths_li = card.select_one('li i.fa-bath, li i.fal.fa-bath')
                if baths_li:
                    baths_li_parent = baths_li.find_parent('li')
                    if baths_li_parent:
                        baths_span = baths_li_parent.select_one('span')
                        if baths_span:
                            baths_text = baths_span.get_text(strip=True)
                            baths = self._parse_int(baths_text)
                
                link_el = card.select_one('a[href*="/for-sale/"], a[href*="/property/"]')
                
                listing_url = None
                if link_el:
                    href = link_el.get('href', '')
                    if href.startswith('/'):
                        listing_url = f"https://www.nigeriapropertycentre.com{href}"
                    elif href.startswith('http'):
                        listing_url = href
                
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
                    'property_type': self._extract_property_type_from_url(listing_url) if listing_url else 'unknown',
                    'listing_date': datetime.now().isoformat(),
                    'scraped_at': datetime.now().isoformat(),
                }
                
                if listing['title'] or listing['price']:
                    listings.append(listing)
                    
            except Exception as e:
                logger.warning(f"[nigeriapropertycentre] Failed to parse card: {e}")
                continue
        
        return listings

    def _parse_privateproperty(self, html: str, source_url: str) -> List[Dict]:
        """UPDATED parser for privateproperty.ng - handles both NGN and USD"""
        soup = BeautifulSoup(html, 'html.parser')
        listings = []
        
        cards = soup.select('div.similar-listings-item, div.result-listings > div')
        
        logger.info(f"Found {len(cards)} listing cards on PrivateProperty")
        
        for card in cards:
            try:
                title_el = card.select_one('.similar-listings-info h2 a, .similar-listings-info h2')
                type_el = card.select_one('.similar-listings-info h3')
                
                # Get price element
                price_el = card.select_one('.similar-listings-price h4')
                price_text = self._text(price_el) if price_el else None
                
                # Handle currency conversion
                clean_price = None
                currency = 'NGN'
                
                if price_text:
                    import re
                    # Check if price is in USD or NGN
                    if '$' in price_text:
                        currency = 'USD'
                        # Extract USD amount
                        usd_match = re.search(r'\$([\d,]+(?:\.\d+)?)', price_text)
                        if usd_match:
                            usd_price = float(usd_match.group(1).replace(',', ''))
                            # Convert USD to NGN (using approximate rate, you might want to use live rate)
                            # For now, store both or use a configurable rate
                            clean_price = usd_price
                            # You could also store USD separately
                    else:
                        # Extract NGN amount
                        ngn_match = re.search(r'₦\s*([\d,]+(?:\.\d+)?)', price_text)
                        if ngn_match:
                            clean_price = float(ngn_match.group(1).replace(',', ''))
                        else:
                            # Try without currency symbol
                            num_match = re.search(r'([\d,]+(?:\.\d+)?)', price_text)
                            if num_match:
                                clean_price = float(num_match.group(1).replace(',', ''))
                
                location_el = card.select_one('.listings-location')
                
                # Extract bedrooms from property-benefit or from text
                beds = None
                baths = None
                
                # Method 1: Look for benefit items with numbers
                benefit_items = card.select('.property-benefit li')
                numbers_found = []
                for item in benefit_items:
                    item_text = self._text(item)
                    if item_text and item_text.isdigit():
                        numbers_found.append(int(item_text))
                
                if len(numbers_found) >= 2:
                    beds = numbers_found[0]
                    baths = numbers_found[1]
                elif len(numbers_found) == 1:
                    beds = numbers_found[0]
                
                # Method 2: Look for bed/bath in title or description
                if not beds:
                    full_text = card.get_text().lower()
                    beds_match = re.search(r'(\d+)\s*bed', full_text)
                    baths_match = re.search(r'(\d+)\s*bath', full_text)
                    if beds_match:
                        beds = int(beds_match.group(1))
                    if baths_match:
                        baths = int(baths_match.group(1))
                
                link_el = card.select_one('.similar-listings-info h2 a, a[href*="/listings/"]')
                
                listing_url = None
                if link_el:
                    href = link_el.get('href', '')
                    if href.startswith('/'):
                        listing_url = f"https://privateproperty.ng{href}"
                    elif href.startswith('http'):
                        listing_url = href
                
                # Extract agent name
                agent_el = card.select_one('.media .media-body h5, .media-body h5')
                agent_name = self._text(agent_el) if agent_el else None
                
                listing = {
                    'source_url': source_url,
                    'listing_url': listing_url,
                    'title': self._text(title_el),
                    'price': clean_price,
                    'price_currency': currency,  # Add currency info
                    'location': self._text(location_el),
                    'address': self._text(location_el),
                    'bedrooms': beds,
                    'bathrooms': baths,
                    'sqft': None,
                    'property_type': self._text(type_el).lower() if type_el else 'unknown',
                    'agent_name': agent_name,
                    'listing_date': datetime.now().isoformat(),
                    'scraped_at': datetime.now().isoformat(),
                }
                
                if listing['title'] or listing['price']:
                    listings.append(listing)
                    
            except Exception as e:
                logger.warning(f"[privateproperty] Failed to parse card: {e}")
                continue
        
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
        
    def convert_currency(self, amount: float, from_currency: str, to_currency: str = 'NGN') -> float:
        """Convert currency using approximate rates (or API for live rates)"""
        if from_currency == to_currency:
            return amount
        
        # Approximate exchange rates (you might want to use an API for live rates)
        rates = {
            'USD': 1500,  # 1 USD = 1500 NGN (approximate)
            'EUR': 1600,
            'GBP': 1900,
        }
        
        if from_currency in rates:
            return amount * rates[from_currency]
        else:
            logger.warning(f"Unknown currency: {from_currency}")
            return amount
    
    def _extract_features_from_text(self, text: str) -> Dict:
        """Extract property features from description text."""
        features = {}
        
        features_list = {
            'swimming_pool': ['pool', 'swimming pool', 'swimmingpool'],
            'gym': ['gym', 'fitness', 'workout'],
            'elevator': ['elevator', 'lift'],
            'security': ['security', 'cctv', 'guarded', '24/7 security'],
            'parking': ['parking', 'car park', 'garage'],
            'furnished': ['furnished', 'fully furnished'],
            'serviced': ['serviced', 'service apartment'],
            'bq': ['bq', 'boys quarter', 'boys quarters'],
            'backup_power': ['generator', 'inverter', 'power backup', '24/7 power'],
            'water_supply': ['borehole', 'well', 'water treatment'],
        }
        
        text_lower = text.lower()
        
        for feature, keywords in features_list.items():
            for keyword in keywords:
                if keyword in text_lower:
                    features[feature] = True
                    break
        
        # Extract numbers (parking spaces, etc.)
        import re
        parking_match = re.search(r'(\d+)\s+parking', text_lower)
        if parking_match:
            features['parking_spaces'] = int(parking_match.group(1))
        
        return features

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
        """Save parsed data to PostgreSQL with new schema fields."""
        if not data:
            logger.warning("No data to save to PostgreSQL")
            return

        cursor = self.pg_conn.cursor()
        
        # Updated property query with new fields
        property_query = """
            INSERT INTO properties (
                property_id, address, city, state, zip_code,
                property_type, bedrooms, bathrooms, living_area_sqft,
                listing_url, source_url, source_site, agent_name, 
                listing_title, scraped_at, price_range, created_at
            ) VALUES %s
            ON CONFLICT (property_id) DO UPDATE SET
                address = EXCLUDED.address,
                property_type = EXCLUDED.property_type,
                bedrooms = EXCLUDED.bedrooms,
                bathrooms = EXCLUDED.bathrooms,
                living_area_sqft = EXCLUDED.living_area_sqft,
                agent_name = EXCLUDED.agent_name,
                listing_title = EXCLUDED.listing_title,
                updated_at = CURRENT_TIMESTAMP
        """
        
        property_values = []
        for item in data:
            # Generate consistent property_id
            property_id = self._generate_property_id(item)
            
            # Determine price range category
            price = item.get('price')
            price_range = self._categorize_price(price) if price else None
            
            # Extract city and state from location string
            city, state = self._extract_location_parts(item.get('location', ''))
            
            property_values.append((
                property_id,
                item.get('address') or item.get('location'),
                city,
                state,
                None,  # zip_code
                item.get('property_type'),
                item.get('bedrooms'),
                item.get('bathrooms'),
                item.get('sqft'),
                item.get('listing_url'),
                item.get('source_url'),
                self._extract_site_name(item.get('source_url', '')),
                item.get('agent_name'),
                item.get('title'),
                datetime.now(),
                price_range,
                datetime.now(),
            ))
        
        if property_values:
            execute_values(cursor, property_query, property_values)
        
        # Updated price query with currency support
        price_query = """
            INSERT INTO price_history (
                property_id, price, currency, price_usd, 
                listing_date, price_per_sqft, status, created_at
            ) VALUES %s
            ON CONFLICT (property_id, listing_date) DO NOTHING
        """
        
        price_values = []
        for item in data:
            property_id = self._generate_property_id(item)
            price = item.get('price')
            currency = item.get('price_currency', 'NGN')
            
            # Convert to USD if needed
            price_usd = None
            if price and currency == 'NGN':
                price_usd = price / 1500  # Approximate conversion
            elif price and currency == 'USD':
                price_usd = price
                # Also convert USD to NGN for consistency
                price = price * 1500
                currency = 'NGN'  # Store NGN as primary
            
            sqft = item.get('sqft')
            price_values.append((
                property_id,
                price,
                currency,
                price_usd,
                datetime.now().date(),
                round(price / sqft, 2) if price and sqft else None,
                'active',
                datetime.now(),
            ))
        
        if price_values:
            execute_values(cursor, price_query, price_values)
        
        # Insert property features if available
        features_query = """
            INSERT INTO property_features (property_id, feature_name, feature_value)
            VALUES %s
            ON CONFLICT (property_id, feature_name) DO NOTHING
        """
        
        features_values = []
        for item in data:
            property_id = self._generate_property_id(item)
            features = item.get('features', {})
            for feature_name, feature_value in features.items():
                features_values.append((
                    property_id,
                    feature_name,
                    str(feature_value) if feature_value else None
                ))
        
        if features_values:
            execute_values(cursor, features_query, features_values)
        
        # Log scraper run
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

    def _generate_property_id(self, item: Dict) -> str:
        """Generate consistent property ID from available data."""
        source = item.get('source_url', 'unknown')
        identifier = (
            item.get('listing_url') or 
            item.get('title') or 
            item.get('location') or 
            ''
        )
        # Create a hash-like ID
        import hashlib
        id_string = f"{source}_{identifier}".lower()
        return hashlib.md5(id_string.encode()).hexdigest()[:32]

    def _categorize_price(self, price: float) -> str:
        """Categorize property price for Nigerian market."""
        if price < 50000000:  # Less than 50M NGN
            return 'Budget'
        elif price < 200000000:  # 50M - 200M NGN
            return 'Mid-Range'
        elif price < 500000000:  # 200M - 500M NGN
            return 'High-End'
        else:  # 500M+ NGN
            return 'Luxury'

    def _extract_location_parts(self, location: str) -> tuple:
        """Extract city and state from Nigerian location string."""
        if not location:
            return (None, None)
        
        # Common Nigerian states
        states = [
            'Lagos', 'Abuja', 'Rivers', 'Ogun', 'Oyo', 'Anambra', 'Enugu',
            'Kano', 'Kaduna', 'Delta', 'Edo', 'Imo', 'Abia', 'Akwa Ibom',
            'Cross River', 'Plateau', 'Benue', 'Niger', 'Kwara', 'Osun',
            'Ondo', 'Ekiti', 'Kogi', 'Nassarawa', 'Bauchi', 'Gombe', 'Yobe',
            'Borno', 'Adamawa', 'Taraba', 'Kebbi', 'Sokoto', 'Zamfara',
            'Katsina', 'Jigawa', 'Bayelsa', 'Ebonyi'
        ]
        
        location_parts = location.split(',')
        city = location_parts[0].strip() if location_parts else None
        state = None
        
        # Look for state in location string
        for s in states:
            if s.lower() in location.lower():
                state = s
                break
        
        return (city, state)

    def _extract_site_name(self, url: str) -> str:
        """Extract site name from URL."""
        if 'propertypro.ng' in url:
            return 'PropertyPro'
        elif 'nigeriapropertycentre.com' in url:
            return 'NigeriaPropertyCentre'
        elif 'privateproperty.ng' in url:
            return 'PrivateProperty'
        else:
            return 'Unknown'

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