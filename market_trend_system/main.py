# main.py
import google.adk as adk
import logging
import time
from market_trend_system import config  # Assuming files are in a package 'market_trend_system'
from market_trend_system.news_scraper_agent import NewsScraperAgent
from market_trend_system.financial_data_agent import FinancialDataAgent
from market_trend_system.data_processor_agent import DataProcessorAgent
from market_trend_system.trend_identification_agent import TrendIdentificationAgent
from market_trend_system.report_generation_agent import ReportGenerationAgent

# Basic logging configuration
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# If not running as a package, adjust imports:
# import config
# from news_scraper_agent import NewsScraperAgent
# ... etc.


def main():
    logger.info("Initializing ADK Runtime and Agents...")
    runtime = adk.Runtime()

    # Register agents
    runtime.register_agent(NewsScraperAgent())
    runtime.register_agent(FinancialDataAgent())
    runtime.register_agent(DataProcessorAgent())
    runtime.register_agent(TrendIdentificationAgent())
    runtime.register_agent(ReportGenerationAgent())

    # Start the runtime (non-blocking)
    runtime.start()
    logger.info("ADK Runtime started.")

    # Determine the session ID for this run
    current_session_id = config.get_current_session_id()
    logger.info(f"Kicking off daily job for session_id: {current_session_id}")

    # Publish the initial event to kick off the process
    runtime.publish(config.EVENT_START_DAILY_JOB, {"session_id": current_session_id})
    logger.info(f"Published {config.EVENT_START_DAILY_JOB} for session {current_session_id}")

    # Keep the main thread alive or perform other tasks.
    # For a simple script, you might just wait for shutdown or a specific event.
    try:
        # Example: Wait for report generated or a timeout
        # This is a simplistic way to "wait" for completion in a hackathon.
        # A more robust system might have the orchestrator listen for the final event.
        timeout_seconds = 1800  # 30 minutes
        start_time = time.time()
        final_event_received = False

        # Define a handler for the final event if you want main to react
        @adk.on_event(config.EVENT_REPORT_GENERATED)
        def final_event_handler(event_name, event_data):
            nonlocal final_event_received  # Python 3 specific
            if event_data.get("session_id") == current_session_id:
                logger.info(
                    f"Main: Received final event {event_name} for session {current_session_id}: {event_data.get('gcs_report_path')}")
                final_event_received = True

        runtime.register_event_handler(config.EVENT_REPORT_GENERATED, final_event_handler)

        while not final_event_received and (time.time() - start_time) < timeout_seconds:
            time.sleep(10)  # Check periodically

        if final_event_received:
            logger.info(f"Process for session {current_session_id} completed successfully.")
        else:
            logger.warning(f"Process for session {current_session_id} timed out or did not complete as expected.")

    except KeyboardInterrupt:
        logger.info("Shutdown requested via KeyboardInterrupt.")
    finally:
        logger.info("Stopping ADK Runtime...")
        runtime.stop()
        logger.info("ADK Runtime stopped.")


if __name__ == "__main__":
    # Ensure GCP credentials are set via GOOGLE_APPLICATION_CREDENTIALS environment variable
    # Fill in GCS_BUCKET_NAME, BQ_PROJECT_ID, ALPHA_VANTAGE_API_KEY in config.py

    # Example check for config (optional, but good for users)
    if "your-gcs-bucket-name" in config.GCS_BUCKET_NAME or \
            "your-gcp-project-id" in config.BQ_PROJECT_ID or \
            "YOUR_ALPHA_VANTAGE_API_KEY" == config.ALPHA_VANTAGE_API_KEY:
        logger.error(
            "CRITICAL: Please update placeholder values in config.py (GCS_BUCKET_NAME, BQ_PROJECT_ID, ALPHA_VANTAGE_API_KEY) before running.")
    else:
        main()