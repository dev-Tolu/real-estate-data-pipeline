import os
import json
import time
import logging
import cloudscraper
from redis import Redis
from datetime import datetime
from minio import Minio
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(level=getattr(logging, os.getenv('SCRAPER_LOG_LEVEL', 'INFO')))
logger = logging.getLogger(__name__)

class RawExtractor:
    def __init__(self):
        self.minio_client = Minio(
            os.getenv('MINIO_ENDPOINT', 'minio:9000'),
            access_key=os.getenv('MINIO_ACCESS_KEY'),
            secret_key=os.getenv('MINIO_SECRET_KEY'),
            secure=False
        )
        self.bucket_raw = os.getenv('S3_BUCKET_RAW', 'realestate-raw-data')
        if not self.minio_client.bucket_exists(self.bucket_raw):
            self.minio_client.make_bucket(self.bucket_raw)

        self.redis_client = Redis(
            host='redis',
            port=6379,
            password=os.getenv('REDIS_PASSWORD'),
            db=0,
            decode_responses=True
        )


        self.source_urls = [u.strip() for u in os.getenv('DATA_SOURCE_URLS', '').split(',') if u.strip()]
        self.session = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            }
        )
        self.rate_limit = 2
        self.last_request_time = 0

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def fetch_page(self, url: str) -> str:
        elapsed = time.time() - self.last_request_time
        if elapsed < (1.0 / self.rate_limit):
            time.sleep((1.0 / self.rate_limit) - elapsed)

        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        self.last_request_time = time.time()
        return response.text

    def extract_and_dump(self, base_url: str):
        job_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        raw_pages = []

        # Simple pagination loop (1 to 5)
        for page in range(1, 6):
            page_url = f"{base_url}?page={page}" if page > 1 else base_url
            try:
                logger.info(f"Fetching {page_url}")
                html_content = self.fetch_page(page_url)
                
                raw_pages.append({
                    "source_url": base_url,
                    "page_url": page_url,
                    "scraped_at": datetime.now().isoformat(),
                    "html": html_content
                })
            except Exception as e:
                logger.error(f"Failed to fetch {page_url}: {e}")
                break

        if raw_pages:
            source_name = base_url.split('//')[-1].replace('/', '_')[:30]
            filename = f"ingest_{job_timestamp}_{source_name}.json"
            
            data_bytes = json.dumps(raw_pages).encode('utf-8')
            self.minio_client.put_object(
                self.bucket_raw, filename,
                data=__import__('io').BytesIO(data_bytes),
                length=len(data_bytes),
                content_type='application/json'
            )
            logger.info(f"Dumped {len(raw_pages)} raw pages to MinIO: {filename}")

    def run(self):
        for url in self.source_urls:
            self.extract_and_dump(url)

if __name__ == "__main__":
    RawExtractor().run()