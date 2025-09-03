"""Data ingestion Lambda function handler - lightweight orchestrator."""

import json
import logging
import os
from typing import Dict, Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def main(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main handler for data ingestion Lambda function.

    Args:
        event: Lambda event data
        context: Lambda context

    Returns:
        Response dictionary with ingestion results
    """
    try:
        logger.info(f"Received event: {json.dumps(event, default=str)}")

        # Environment variables
        raw_bucket = os.environ.get("RAW_BUCKET")
        environment = os.environ.get("ENVIRONMENT")

        logger.info(f"Processing data ingestion for environment: {environment}")

        # Parse expected inputs with safe defaults
        data_source = str(event.get("data_source", "yahoo_finance"))
        data_type = str(event.get("data_type", "prices"))
        symbols = event.get("symbols", [])
        if not isinstance(symbols, list):
            symbols = []
        valid_symbols = [s for s in symbols if isinstance(s, str) and s.strip()]
        invalid_symbols = [s for s in symbols if not isinstance(s, str) or not str(s).strip()]
        period = str(event.get("period", "1y"))
        interval = str(event.get("interval", "1d"))
        domain = str(event.get("domain", "market"))
        table_name = str(event.get("table_name", "prices"))
        file_format = str(event.get("file_format", "parquet"))

        # Placeholder: here you would route based on data_source/data_type
        # and fetch data using the appropriate client, then write to S3 raw bucket.
        processed_records = 0

        # Process the event (placeholder response)
        result = {
            "statusCode": 200,
            "body": {
                "message": "Data ingestion completed successfully",
                "data_source": data_source,
                "data_type": data_type,
                "symbols_requested": symbols,
                "symbols_processed": valid_symbols,
                "invalid_symbols": invalid_symbols,
                "period": period,
                "interval": interval,
                "domain": domain,
                "table_name": table_name,
                "file_format": file_format,
                "environment": environment,
                "raw_bucket": raw_bucket,
                "processed_records": processed_records,  # Placeholder
            },
        }

        logger.info("Data ingestion completed successfully")
        return result

    except Exception as e:
        logger.error(f"Error in data ingestion: {str(e)}")
        return {
            "statusCode": 500,
            "body": {"error": str(e), "message": "Data ingestion failed"},
        }
