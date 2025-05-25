# config.py
import datetime

# GCP Configuration
GCS_BUCKET_NAME = "your-gcs-bucket-name-for-hackathon"  #  obligatorio: Reemplazar!
BQ_PROJECT_ID = "your-gcp-project-id"                # obligatorio: Reemplazar!
BQ_DATASET_ID = "market_trend_analysis_dataset" # Se creará si no existe

# Alpha Vantage
ALPHA_VANTAGE_API_KEY = "DVC1YCBI5J4F7TTY"  # obligatorio: Reemplazar!

# News Feeds
NEWS_RSS_FEEDS = {
    "Reuters Top News": "http://feeds.reuters.com/reuters/topNews",
    "AP News Top": "https://rss.app/feeds/aXl0FxI5n0PgB5OR.xml", # AP News RSS can be tricky, using a 3rd party aggregator if official is unstable
    "Bloomberg General": "https://feeds.bloomberg.com/bbiz/sitemap_news.xml", # General news sitemap
    "Yahoo Finance": "https://finance.yahoo.com/news/rssindex"
}
# Note: Some RSS feeds might require user-agents or have other restrictions. Test them.

# Stock/Index Symbols
STOCK_SYMBOLS = ["SPY", "QQQ", "AAPL", "GOOGL", "MSFT"] # GOOGL instead of GOOG for Alpha Vantage

# GCS Paths
RAW_DATA_GCS_PATH_PREFIX = "raw_data"
REPORTS_GCS_PATH_PREFIX = "reports"

# BigQuery Table Names
BQ_TABLE_PROCESSED_NEWS = "processed_news"
BQ_TABLE_PROCESSED_FINANCIALS = "processed_financials"

# Event Types (using strings for ADK event names)
EVENT_START_DAILY_JOB = "start_daily_job" # Renamed for clarity, carries session_id
EVENT_NEWS_ARTICLE_RAW = "news_article_raw" # Singular for clarity
EVENT_FINANCIAL_DATA_POINT_RAW = "financial_data_point_raw" # Singular for clarity

EVENT_ALL_RAW_NEWS_GATHERED = "all_raw_news_gathered_for_session"
EVENT_ALL_RAW_FINANCIAL_GATHERED = "all_raw_financial_gathered_for_session"

EVENT_NEWS_PROCESSED_FOR_SESSION = "news_processed_for_session"
EVENT_FINANCIAL_PROCESSED_FOR_SESSION = "financial_processed_for_session"

EVENT_TRENDS_IDENTIFIED_FOR_SESSION = "trends_identified_for_session"
EVENT_REPORT_GENERATED = "report_generated_for_session"

# Trend Identification
FINANCIAL_ANOMALY_THRESHOLD_PERCENT = 5.0 # e.g., 5% change

# Utility
def get_current_session_id():
    return datetime.datetime.utcnow().strftime('%Y-%m-%d')