# trend_identification_agent.py
import google.adk as adk
import logging
import nltk
import pandas as pd
from google.cloud import bigquery
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import datetime
from . import config

logger = logging.getLogger(__name__)

try:
    nltk.data.find('sentiment/vader_lexicon.zip')
except LookupError:
    nltk.download('vader_lexicon', quiet=True)

try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt', quiet=True)


class TrendIdentificationAgent(adk.Agent):
    def __init__(self):
        super().__init__()
        self.bq_client = bigquery.Client(project=config.BQ_PROJECT_ID)
        self.sid = SentimentIntensityAnalyzer()

        # State to track if both news and financial data for a session are processed
        self.processed_session_status = {}  # { "session_id": {"news_analyzed": False, "financial_analyzed": False}}

        self.register_event_handler(config.EVENT_NEWS_PROCESSED_FOR_SESSION, self.handle_news_processed_for_session)
        self.register_event_handler(config.EVENT_FINANCIAL_PROCESSED_FOR_SESSION,
                                    self.handle_financial_processed_for_session)

    def _get_sentiment_label(self, compound_score):
        if compound_score >= 0.05:
            return 'positive'
        elif compound_score <= -0.05:
            return 'negative'
        else:
            return 'neutral'

    def _perform_sentiment_analysis(self, session_id):
        logger.info(f"[{session_id}] Performing sentiment analysis on news.")
        news_table_ref = f"{config.BQ_PROJECT_ID}.{config.BQ_DATASET_ID}.{config.BQ_TABLE_PROCESSED_NEWS}"

        # Query for articles in the session that haven't been analyzed yet
        query = f"""
            SELECT article_id, title, summary_cleaned
            FROM `{news_table_ref}`
            WHERE session_id = @session_id AND sentiment_score IS NULL
        """
        query_params = [bigquery.ScalarQueryParameter("session_id", "STRING", session_id)]
        job_config = bigquery.QueryJobConfig(query_parameters=query_params)

        rows_to_update = []
        try:
            query_job = self.bq_client.query(query, job_config=job_config)
            for row in query_job:  # Iterate over BQ results
                text_to_analyze = (row.title or "") + " " + (row.summary_cleaned or "")
                if not text_to_analyze.strip():
                    sentiment_score = 0.0
                    sentiment_label = 'neutral'
                else:
                    scores = self.sid.polarity_scores(text_to_analyze)
                    sentiment_score = scores['compound']
                    sentiment_label = self._get_sentiment_label(sentiment_score)

                rows_to_update.append({
                    "article_id": row.article_id,
                    "sentiment_score": sentiment_score,
                    "sentiment_label": sentiment_label
                })
            logger.info(f"[{session_id}] Calculated sentiment for {len(rows_to_update)} news articles.")

            # Batch update BQ (MERGE is more robust but more complex to construct dynamically without temp tables)
            # For hackathon, individual updates or a simpler update statement might be acceptable.
            # Here, we'll use a MERGE statement. This assumes smaller batches.
            # For very large numbers of rows, consider loading to a temp table and then MERGE.
            if rows_to_update:
                # Building a MERGE statement with VALUES part.
                # This can get large. If >1MB query, BQ might reject.
                # Alternative: DataFrame + to_gbq to staging, then MERGE.
                values_clause = ",\n".join(
                    [f"('{item['article_id']}', {item['sentiment_score']}, '{item['sentiment_label']}')" for item in
                     rows_to_update])

                if not values_clause:  # Should not happen if rows_to_update is not empty
                    logger.info(f"[{session_id}] No rows to update sentiment for.")
                    return True

                merge_sql = f"""
                MERGE `{news_table_ref}` T
                USING (VALUES {values_clause}) S(article_id, sentiment_score, sentiment_label)
                ON T.article_id = S.article_id AND T.session_id = @session_id
                WHEN MATCHED THEN
                  UPDATE SET T.sentiment_score = S.sentiment_score, T.sentiment_label = S.sentiment_label
                """
                merge_job_config = bigquery.QueryJobConfig(query_parameters=[
                    bigquery.ScalarQueryParameter("session_id", "STRING", session_id)
                ])
                self.bq_client.query(merge_sql, job_config=merge_job_config).result()  # Wait for completion
                logger.info(
                    f"[{session_id}] Successfully updated sentiment scores in BQ for {len(rows_to_update)} articles.")
            return True
        except Exception as e:
            logger.error(f"[{session_id}] Error during sentiment analysis or BQ update: {e}", exc_info=True)
            return False

    def _perform_financial_anomaly_detection(self, session_id):
        logger.info(f"[{session_id}] Performing financial anomaly detection.")
        financial_table_ref = f"{config.BQ_PROJECT_ID}.{config.BQ_DATASET_ID}.{config.BQ_TABLE_PROCESSED_FINANCIALS}"

        # Query current and previous day's close prices for each stock in the session
        # Note: session_id is 'YYYY-MM-DD'. BQ date functions can work with this.
        query = f"""
            WITH RankedPrices AS (
                SELECT
                    symbol,
                    date,
                    close_price,
                    LAG(close_price, 1, NULL) OVER (PARTITION BY symbol ORDER BY date ASC) as prev_close_price,
                    session_id
                FROM `{financial_table_ref}`
                -- Widen the date range slightly to ensure we get previous day if session_id is a market holiday follow-up
                WHERE date <= PARSE_DATE('%Y-%m-%d', @session_id) 
                  AND date >= DATE_SUB(PARSE_DATE('%Y-%m-%d', @session_id), INTERVAL 7 DAY)
                  AND symbol IN UNNEST(@stock_symbols) -- Process only relevant symbols for this session
            )
            SELECT
                symbol,
                date,
                close_price,
                prev_close_price
            FROM RankedPrices
            WHERE session_id = @session_id -- Filter to the current session after LAG
              AND prev_close_price IS NOT NULL -- Ensure we have a previous day to compare
              AND daily_change_pct IS NULL -- Only process if not already done
        """
        query_params = [
            bigquery.ScalarQueryParameter("session_id", "STRING", session_id),
            bigquery.ArrayQueryParameter("stock_symbols", "STRING", config.STOCK_SYMBOLS)
        ]
        job_config = bigquery.QueryJobConfig(query_parameters=query_params)

        rows_to_update = []
        try:
            query_job = self.bq_client.query(query, job_config=job_config)
            for row in query_job:
                if row.prev_close_price and row.prev_close_price != 0:
                    daily_change_pct = ((row.close_price - row.prev_close_price) / row.prev_close_price)
                else:
                    daily_change_pct = 0.0

                is_anomaly = abs(daily_change_pct * 100) > config.FINANCIAL_ANOMALY_THRESHOLD_PERCENT

                rows_to_update.append({
                    "symbol": row.symbol,
                    "date_str": row.date.strftime('%Y-%m-%d'),  # For matching in MERGE
                    "daily_change_pct": daily_change_pct,
                    "is_anomaly": is_anomaly
                })
            logger.info(f"[{session_id}] Calculated financial anomalies for {len(rows_to_update)} data points.")

            if rows_to_update:
                values_clause = ",\n".join([
                                               f"('{item['symbol']}', PARSE_DATE('%Y-%m-%d','{item['date_str']}'), {item['daily_change_pct']}, {str(item['is_anomaly']).upper()})"
                                               for item in rows_to_update])

                if not values_clause:
                    logger.info(f"[{session_id}] No rows to update financial anomalies for.")
                    return True

                merge_sql = f"""
                MERGE `{financial_table_ref}` T
                USING (VALUES {values_clause}) S(symbol, date, daily_change_pct, is_anomaly)
                ON T.symbol = S.symbol AND T.date = S.date AND T.session_id = @session_id
                WHEN MATCHED THEN
                  UPDATE SET T.daily_change_pct = S.daily_change_pct, T.is_anomaly = S.is_anomaly
                """
                merge_job_config = bigquery.QueryJobConfig(query_parameters=[
                    bigquery.ScalarQueryParameter("session_id", "STRING", session_id)
                ])
                self.bq_client.query(merge_sql, job_config=merge_job_config).result()
                logger.info(
                    f"[{session_id}] Successfully updated financial anomalies in BQ for {len(rows_to_update)} data points.")
            return True
        except Exception as e:
            logger.error(f"[{session_id}] Error during financial anomaly detection or BQ update: {e}", exc_info=True)
            return False

    def _try_to_finalize_session_analysis(self, session_id):
        status = self.processed_session_status.get(session_id, {})
        if status.get("news_analyzed") and status.get("financial_analyzed"):
            logger.info(
                f"[{session_id}] Both news and financial analysis complete. Publishing trends identified event.")
            self.publish(config.EVENT_TRENDS_IDENTIFIED_FOR_SESSION, {"session_id": session_id})
            del self.processed_session_status[session_id]  # Clean up

    def handle_news_processed_for_session(self, event_name, event_data):
        session_id = event_data["session_id"]
        logger.info(f"[{session_id}] Received news processed for session. Starting sentiment analysis.")

        analysis_success = self._perform_sentiment_analysis(session_id)

        self.processed_session_status.setdefault(session_id, {})["news_analyzed"] = analysis_success
        if not analysis_success:
            logger.error(f"[{session_id}] Sentiment analysis failed. Trend identification might be incomplete.")

        self._try_to_finalize_session_analysis(session_id)

    def handle_financial_processed_for_session(self, event_name, event_data):
        session_id = event_data["session_id"]
        logger.info(f"[{session_id}] Received financial data processed for session. Starting anomaly detection.")

        analysis_success = self._perform_financial_anomaly_detection(session_id)

        self.processed_session_status.setdefault(session_id, {})["financial_analyzed"] = analysis_success
        if not analysis_success:
            logger.error(
                f"[{session_id}] Financial anomaly detection failed. Trend identification might be incomplete.")

        self._try_to_finalize_session_analysis(session_id)