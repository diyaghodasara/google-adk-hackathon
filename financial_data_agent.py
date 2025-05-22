# financial_data_agent.py
import adk
import requests
import datetime
import time
import logging
from . import config

logger = logging.getLogger(__name__)


class FinancialDataAgent(adk.Agent):
    def __init__(self):
        super().__init__()
        self.register_event_handler(config.EVENT_START_DAILY_JOB, self.handle_start_fetching)

    def handle_start_fetching(self, event_name, event_data):
        session_id = event_data.get("session_id", config.get_current_session_id())
        logger.info(f"[{session_id}] FinancialDataAgent: Received {event_name}, starting fetch.")

        symbols_fetched_count = 0
        for symbol in config.STOCK_SYMBOLS:
            logger.info(f"[{session_id}] Fetching data for {symbol}")
            try:
                # Alpha Vantage TIME_SERIES_DAILY returns last 100 days, good for % change calculation later
                params = {
                    "function": "TIME_SERIES_DAILY",
                    "symbol": symbol,
                    "apikey": config.ALPHA_VANTAGE_API_KEY,
                    "outputsize": "compact"  # Last 100 data points
                }
                response = requests.get("https://www.alphavantage.co/query", params=params)
                response.raise_for_status()  # Raise HTTPError for bad responses (4XX or 5XX)
                data = response.json()

                if "Time Series (Daily)" not in data:
                    logger.warning(
                        f"[{session_id}] 'Time Series (Daily)' not found for {symbol}. Response: {data.get('Information', data.get('Note', str(data)[:200]))}")
                    # Alpha Vantage API limit: 5 calls/min, 100/day. If 'Note' is present, it's likely a rate limit.
                    if "Note" in data or "Information" in data and "limit" in data.get("Information", "").lower():
                        logger.warning(f"[{session_id}] Alpha Vantage API limit likely hit. Pausing for 60s.")
                        time.sleep(61)  # Wait for more than a minute before retrying or next symbol
                        # Potentially re-try current symbol or just skip to next after pause
                    continue

                time_series = data["Time Series (Daily)"]
                # We are interested in data for the session_id date.
                # Alpha Vantage might not have data for the exact session_id (e.g., weekend, holiday)
                # We'll let DataProcessorAgent handle picking the relevant date or latest.
                # For now, publish all fetched data points within the compact series.
                # Or, more aligned with "daily data", pick the latest available.

                # Let's pick the latest available date from the series.
                # The dates are keys in "YYYY-MM-DD" format.
                if not time_series:
                    logger.warning(f"[{session_id}] Empty time series for {symbol}.")
                    continue

                latest_date_str = sorted(time_series.keys(), reverse=True)[0]
                latest_data_point = time_series[latest_date_str]

                # For this example, we only process the latest data point.
                # If you need T-1 for immediate calc, you'd grab two points.
                # But TrendID agent will query BQ for T-1.

                financial_data_point = {
                    "symbol": symbol,
                    "date": latest_date_str,  # This is the actual market date
                    "open": float(latest_data_point["1. open"]),
                    "high": float(latest_data_point["2. high"]),
                    "low": float(latest_data_point["3. low"]),
                    "close_price": float(latest_data_point["4. close"]),
                    "volume": int(latest_data_point["5. volume"]),
                    "fetched_at": datetime.datetime.utcnow().isoformat()
                }

                raw_event_data = {
                    "session_id": session_id,
                    **financial_data_point
                }
                self.publish(config.EVENT_FINANCIAL_DATA_POINT_RAW, raw_event_data)
                symbols_fetched_count += 1
                logger.debug(f"[{session_id}] Fetched and published for {symbol} on {latest_date_str}")

                # Alpha Vantage Free Tier: 5 calls per minute.
                if len(config.STOCK_SYMBOLS) > 4:  # Add delay if more than 4 to avoid rate limit
                    logger.info(f"[{session_id}] Pausing for 15s due to Alpha Vantage rate limits...")
                    time.sleep(15)

            except requests.exceptions.RequestException as e:
                logger.error(f"[{session_id}] HTTP Error fetching {symbol}: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"[{session_id}] Error processing {symbol}: {e}", exc_info=True)

        logger.info(
            f"[{session_id}] FinancialDataAgent: Finished all fetching. Total symbols processed: {symbols_fetched_count}")
        self.publish(config.EVENT_ALL_RAW_FINANCIAL_GATHERED,
                     {"session_id": session_id, "count": symbols_fetched_count})