# report_generation_agent.py
import google.adk as adk
import logging
import jinja2
import pandas as pd
from google.cloud import bigquery, storage
import datetime
import matplotlib.pyplot as plt
import io
import base64
from . import config
import os

logger = logging.getLogger(__name__)


class ReportGenerationAgent(adk.Agent):
    def __init__(self):
        super().__init__()
        self.bq_client = bigquery.Client(project=config.BQ_PROJECT_ID)
        self.gcs_client = storage.Client()
        self.gcs_bucket = self.gcs_client.bucket(config.GCS_BUCKET_NAME)

        # Setup Jinja2 environment
        template_loader = jinja2.FileSystemLoader(searchpath=os.path.join(os.path.dirname(__file__), "templates"))
        self.jinja_env = jinja2.Environment(loader=template_loader)

        self.register_event_handler(config.EVENT_TRENDS_IDENTIFIED_FOR_SESSION, self.handle_trends_identified)

    def _generate_sentiment_chart_b64(self, sentiment_summary_df):
        if sentiment_summary_df.empty:
            return None
        try:
            fig, ax = plt.subplots()
            sentiment_summary_df.plot(kind='bar', x='sentiment_label', y='count', ax=ax, legend=False)
            ax.set_title('News Sentiment Distribution')
            ax.set_xlabel('Sentiment')
            ax.set_ylabel('Number of Articles')
            plt.tight_layout()

            img_bytes = io.BytesIO()
            plt.savefig(img_bytes, format='png')
            plt.close(fig)  # Close the figure to free memory
            img_bytes.seek(0)
            return base64.b64encode(img_bytes.read()).decode('utf-8')
        except Exception as e:
            logger.error(f"Error generating sentiment chart: {e}", exc_info=True)
            return None

    def handle_trends_identified(self, event_name, event_data):
        session_id = event_data["session_id"]
        logger.info(f"[{session_id}] Received trends identified. Generating report.")

        report_data = {"session_id": session_id}

        try:
            # 1. Fetch News Data and Sentiment Summary
            news_table = f"{config.BQ_PROJECT_ID}.{config.BQ_DATASET_ID}.{config.BQ_TABLE_PROCESSED_NEWS}"
            news_query = f"""
                SELECT title, url, summary_cleaned, sentiment_label, sentiment_score
                FROM `{news_table}`
                WHERE session_id = @session_id
                ORDER BY sentiment_score DESC NULLS LAST
            """  # Order by score to get top articles easily
            news_query_params = [bigquery.ScalarQueryParameter("session_id", "STRING", session_id)]
            news_job_config = bigquery.QueryJobConfig(query_parameters=news_query_params)
            news_df = self.bq_client.query(news_query, job_config=news_job_config).to_dataframe()

            if not news_df.empty:
                report_data["top_articles"] = news_df.head(5).to_dict('records')
                sentiment_counts = news_df['sentiment_label'].value_counts().reset_index()
                sentiment_counts.columns = ['sentiment_label', 'count']
                report_data["sentiment_summary"] = sentiment_counts.set_index('sentiment_label')['count'].to_dict()
                report_data["sentiment_chart_b64"] = self._generate_sentiment_chart_b64(sentiment_counts)
            else:
                report_data["top_articles"] = []
                report_data["sentiment_summary"] = {'positive': 0, 'negative': 0, 'neutral': 0}
                report_data["sentiment_chart_b64"] = None

            # 2. Fetch Financial Anomalies
            financial_table = f"{config.BQ_PROJECT_ID}.{config.BQ_DATASET_ID}.{config.BQ_TABLE_PROCESSED_FINANCIALS}"
            financial_query = f"""
                SELECT symbol, daily_change_pct
                FROM `{financial_table}`
                WHERE session_id = @session_id AND is_anomaly = TRUE
            """
            financial_query_params = [bigquery.ScalarQueryParameter("session_id", "STRING", session_id)]
            financial_job_config = bigquery.QueryJobConfig(query_parameters=financial_query_params)
            anomalies_df = self.bq_client.query(financial_query, job_config=financial_job_config).to_dataframe()

            report_data["financial_anomalies"] = anomalies_df.to_dict('records') if not anomalies_df.empty else []

            # 3. Render HTML Report
            template = self.jinja_env.get_template("report_template.html")
            html_report = template.render(report_data)

            # 4. Save report to GCS
            report_filename = f"{session_id}_market_trend_report.html"
            gcs_report_path = f"{config.REPORTS_GCS_PATH_PREFIX}/{report_filename}"
            blob = self.gcs_bucket.blob(gcs_report_path)
            blob.upload_from_string(html_report, content_type='text/html')
            full_gcs_path = f"gs://{config.GCS_BUCKET_NAME}/{gcs_report_path}"
            logger.info(f"[{session_id}] Report generated and saved to {full_gcs_path}")

            # 5. Publish report_generated event
            self.publish(config.EVENT_REPORT_GENERATED, {
                "session_id": session_id,
                "gcs_report_path": full_gcs_path
            })

        except Exception as e:
            logger.error(f"[{session_id}] Error generating report: {e}", exc_info=True)