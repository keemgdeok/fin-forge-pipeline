"""Data ingestion Lambda function handler - lightweight orchestrator."""

import json
import os
from typing import Dict, Any, List

from shared.models.events import DataIngestionEvent
from shared.utils.logger import get_logger, extract_correlation_id

logger = get_logger(__name__)


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
        # Use invocation-scoped logger to avoid correlation id leakage
        corr_id = extract_correlation_id(event)
        log = get_logger(__name__, correlation_id=corr_id) if corr_id else logger
        log.info(f"Received event: {json.dumps(event, default=str)}")

        # Environment variables
        raw_bucket = os.environ.get("RAW_BUCKET")
        environment = os.environ.get("ENVIRONMENT")

        log.info(f"Processing data ingestion for environment: {environment}")

        # Parse expected inputs with safe defaults using typed model
        model = DataIngestionEvent.model_validate(event)
        data_source = model.data_source
        data_type = model.data_type
        symbols_raw: List[Any] = event.get("symbols", []) if isinstance(event, dict) else []
        valid_symbols = model.symbols
        invalid_symbols = [s for s in symbols_raw if not isinstance(s, str) or not str(s).strip()]
        period = model.period
        interval = model.interval
        domain = model.domain
        table_name = model.table_name
        file_format = model.file_format

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
                "symbols_requested": symbols_raw,
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

        log.info("Data ingestion completed successfully")
        return result

    except Exception as e:
        # Include stack trace for better observability
        log = logger
        log.exception("Error in data ingestion")
        return {
            "statusCode": 500,
            "body": {"error": str(e), "message": "Data ingestion failed"},
        }
