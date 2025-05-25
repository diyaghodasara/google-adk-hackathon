# news_scraper_agent.py
import google.adk as adk
import feedparser
import json
import datetime
import uuid
import time
import logging
from google.cloud import storage
from . import config  # Use relative import if part of a package

logger = logging.getLogger(__name__)


class NewsScraperAgent(adk.Agent):
    def __init__(self):
        super().__init__()
        self.gcs_client = storage.Client()
        self.bucket = self.gcs_client.bucket(config.GCS_BUCKET_NAME)
        self.register_event_handler(config.EVENT_START_DAILY_JOB, self.handle_start_scraping)

    def handle_start_scraping(self, event_name, event_data):
        session_id = event_data.get("session_id", config.get_current_session_id())
        logger.info(f"[{session_id}] NewsScraperAgent: Received {event_name}, starting scrape.")

        articles_scraped_count = 0
        for feed_name, feed_url in config.NEWS_RSS_FEEDS.items():
            logger.info(f"[{session_id}] Scraping {feed_name} from {feed_url}")
            try:
                feed = feedparser.parse(feed_url)
                for entry in feed.entries:
                    article_id = str(uuid.uuid4())

                    published_parsed = None
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        published_parsed = datetime.datetime.fromtimestamp(
                            time.mktime(entry.published_parsed)).isoformat()
                    elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                        published_parsed = datetime.datetime.fromtimestamp(
                            time.mktime(entry.updated_parsed)).isoformat()
                    else:
                        published_parsed = datetime.datetime.utcnow().isoformat()  # Fallback

                    article_data = {
                        "article_id": article_id,
                        "title": getattr(entry, 'title', None),
                        "url": getattr(entry, 'link', None),
                        "summary": getattr(entry, 'summary', getattr(entry, 'description', None)),
                        "published_at_str": published_parsed,  # Storing as string for BQ compatibility
                        "feed_source": feed_name,
                        "scraped_at": datetime.datetime.utcnow().isoformat(),
                    }

                    # Store raw article to GCS
                    gcs_path = f"{config.RAW_DATA_GCS_PATH_PREFIX}/news/{session_id}/{article_id}.json"
                    blob = self.bucket.blob(gcs_path)
                    blob.upload_from_string(json.dumps(article_data, indent=2), content_type='application/json')

                    # Publish raw article event
                    raw_event_data = {
                        "session_id": session_id,
                        "gcs_raw_path": f"gs://{config.GCS_BUCKET_NAME}/{gcs_path}",
                        **article_data  # unpack article data into the event
                    }
                    self.publish(config.EVENT_NEWS_ARTICLE_RAW, raw_event_data)
                    articles_scraped_count += 1
                    logger.debug(f"[{session_id}] Scraped and published: {article_data.get('title')[:50]}...")
                logger.info(f"[{session_id}] Finished scraping {feed_name}. Found {len(feed.entries)} entries.")
            except Exception as e:
                logger.error(f"[{session_id}] Error scraping {feed_name}: {e}", exc_info=True)

        logger.info(f"[{session_id}] NewsScraperAgent: Finished all scraping. Total articles: {articles_scraped_count}")
        self.publish(config.EVENT_ALL_RAW_NEWS_GATHERED, {"session_id": session_id, "count": articles_scraped_count})