# data_processor_agent.py
import adk
import re
import html
import logging
from google.cloud import bigquery
import datetime
from . import config

logger = logging.getLogger(__name__)


class DataProcessorAgent(adk.Agent):
    def __init__(self):
        super().__init__()
        self.bq_client = bigquery.Client(project=config.BQ_PROJECT_ID)
        self._ensure_bq_dataset_exists()
        self._ensure_bq_tables_exist()

        # State to track completion for a session
        self.raw_completion_status = {}  # { "session_id": {"news": False, "financial": False}}

        self.register_event_handler(config.EVENT_NEWS_ARTICLE_RAW, self.handle_raw_news)
        self.register_event_handler(config.EVENT_FINANCIAL_DATA_POINT_RAW, self.handle_raw_financial_data)
        self.register_event_handler(config.EVENT_ALL_RAW_NEWS_GATHERED, self.handle_all_raw_news_gathered)
        self.register_event_handler(config.EVENT_ALL_RAW_FINANCIAL_GATHERED, self.handle_all_raw_financial_gathered)

    def _ensure_bq_dataset_exists(self):
        dataset_id = f"{config.BQ_PROJECT_ID}.{config.BQ_DATASET_ID}"
        try:
            self.bq_client.get_dataset(dataset_id)
            logger.info(f"BigQuery dataset {dataset_id} already exists.")
        except Exception:  # google.cloud.exceptions.NotFound
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"  # Or your preferred location
            self.bq_client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Created BigQuery dataset {dataset_id}")

    def _ensure_bq_tables_exist(self):
        # Processed News Table
        news_table_id = f"{config.BQ_PROJECT_ID}.{config.BQ_DATASET_ID}.{config.BQ_TABLE_PROCESSED_NEWS}"
        news_schema = [
            bigquery.SchemaField("session_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("article_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("summary_cleaned", "STRING"),
            bigquery.SchemaField("published_at", "TIMESTAMP"),  # Will store parsed datetime
            bigquery.SchemaField("feed_source", "STRING"),
            bigquery.SchemaField("gcs_raw_path", "STRING"),
            bigquery.SchemaField("processed_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("sentiment_score", "FLOAT"),  # Populated by TrendIdentificationAgent
            bigquery.SchemaField("sentiment_label", "STRING"),  # Populated by TrendIdentificationAgent
        ]
        try:
            self.bq_client.get_table(news_table_id)
        except Exception:
            table = bigquery.Table(news_table_id, schema=news_schema)
            self.bq_client.create_table(table)
            logger.info(f"Created BigQuery table {news_table_id}")

        # Processed Financials Table
        financials_table_id = f"{config.BQ_PROJECT_ID}.{config.BQ_DATASET_ID}.{config.BQ_TABLE_PROCESSED_FINANCIALS}"
        financials_schema = [
            bigquery.SchemaField("session_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),  # Market date
            bigquery.SchemaField("open_price", "FLOAT"),
            bigquery.SchemaField("high_price", "FLOAT"),
            bigquery.SchemaField("low_price", "FLOAT"),
            bigquery.SchemaField("close_price", "FLOAT"),
            bigquery.SchemaField("volume", "INTEGER"),
            bigquery.SchemaField("processed_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("daily_change_pct", "FLOAT"),  # Populated by TrendIdentificationAgent
            bigquery.SchemaField("is_anomaly", "BOOLEAN"),  # Populated by TrendIdentificationAgent
        ]
        try:
            self.bq_client.get_table(financials_table_id)
        except Exception:
            table = bigquery.Table(financials_table_id, schema=financials_schema)
            self.bq_client.create_table(table)
            logger.info(f"Created BigQuery table {financials_table_id}")

    def _clean_text(self, text):
        if not text:
            return ""
        text = html.unescape(text)  # Unescape HTML entities like &
        text = re.sub(r'<[^>]+>', '', text)  # Remove HTML tags
        text = ' '.join(text.split())  # Remove excessive whitespace
        return text

    def handle_raw_news(self, event_name, event_data):
        session_id = event_data["session_id"]
        logger.debug(f"[{session_id}] Processing raw news: {event_data.get('title', 'N/A')[:50]}...")

        try:
            # Convert published_at_str to datetime object then to BQ compatible TIMESTAMP
            published_dt = datetime.datetime.fromisoformat(event_data["published_at_str"])

            processed_article = {
                "session_id": session_id,
                "article_id": event_data["article_id"],
                "title": self._clean_text(event_data.get("title")),
                "url": event_data.get("url"),
                "summary_cleaned": self._clean_text(event_data.get("summary")),
                "published_at": published_dt,
                "feed_source": event_data.get("feed_source"),
                "gcs_raw_path": event_data.get("gcs_raw_path"),
                "processed_at": datetime.datetime.utcnow(),
                # sentiment fields will be null initially
            }
            table_id = f"{config.BQ_PROJECT_ID}.{config.BQ_DATASET_ID}.{config.BQ_TABLE_PROCESSED_NEWS}"
            errors = self.bq_client.insert_rows_json(table_id, [processed_article])
            if errors:
                logger.error(f"[{session_id}] BQ insert errors for news {event_data['article_id']}: {errors}")
            else:
                logger.debug(f"[{session_id}] Successfully inserted processed news {event_data['article_id']} to BQ.")
        except Exception as e:
            logger.error(f"[{session_id}] Error processing raw news {event_data.get('article_id')}: {e}", exc_info=True)

    def handle_raw_financial_data(self, event_name, event_data):
        session_id = event_data["session_id"]
        logger.debug(f"[{session_id}] Processing raw financial data for {event_data.get('symbol')}")

        try:
            # Convert date string to date object
            market_date_obj = datetime.datetime.strptime(event_data["date"], "%Y-%m-%d").date()

            processed_financial_data = {
                "session_id": session_id,
                "symbol": event_data["symbol"],
                "date": market_date_obj,
                "open_price": float(event_data["open"]),
                "high_price": float(event_data["high"]),
                "low_price": float(event_data["low"]),
                "close_price": float(event_data["close_price"]),
                "volume": int(event_data["volume"]),
                "processed_at": datetime.datetime.utcnow(),
                # daily_change_pct and is_anomaly will be null initially
            }
            table_id = f"{config.BQ_PROJECT_ID}.{config.BQ_DATASET_ID}.{config.BQ_TABLE_PROCESSED_FINANCIALS}"
            errors = self.bq_client.insert_rows_json(table_id, [processed_financial_data])
            if errors:
                logger.error(f"[{session_id}] BQ insert errors for financial data {event_data['symbol']}: {errors}")
            else:
                logger.debug(
                    f"[{session_id}] Successfully inserted processed financial data for {event_data['symbol']} to BQ.")
        except Exception as e:
            logger.error(f"[{session_id}] Error processing raw financial data {event_data.get('symbol')}: {e}",
                         exc_info=True)

    def _check_and_publish_processed_session(self, session_id):
        if session_id in self.raw_completion_status and \
                self.raw_completion_status[session_id].get("news", False) and \
                self.raw_completion_status[session_id].get("financial", False):
            logger.info(
                f"[{session_id}] Both news and financial raw data gathered. Publishing processed session events.")
            self.publish(config.EVENT_NEWS_PROCESSED_FOR_SESSION, {"session_id": session_id})
            self.publish(config.EVENT_FINANCIAL_PROCESSED_FOR_SESSION, {"session_id": session_id})

            # Clean up status for this session
            del self.raw_completion_status[session_id]

    def handle_all_raw_news_gathered(self, event_name, event_data):
        session_id = event_data["session_id"]
        logger.info(f"[{session_id}] All raw news gathered signal received ({event_data.get('count', 0)} articles).")
        self.raw_completion_status.setdefault(session_id, {})["news"] = True
        self._check_and_publish_processed_session(session_id)

    def handle_all_raw_financial_gathered(self, event_name, event_data):
        session_id = event_data["session_id"]
        logger.info(
            f"[{session_id}] All raw financial data gathered signal received ({event_data.get('count', 0)} symbols).")
        self.raw_completion_status.setdefault(session_id, {})["financial"] = True
        self._check_and_publish_processed_session(session_id)