"""SQS Worker Lambda: processes one or few symbols and writes RAW to S3.

Consumes SQS event with records where each body contains the same
payload expected by data_ingestion.handler.main (domain, table_name,
symbols, period, interval, file_format, etc.).

Implements partial batch failure semantics.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List

from shared.utils.logger import get_logger, extract_correlation_id
from shared.ingestion.service import process_event


logger = get_logger(__name__)


def main(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Entry point for SQS batch processing with partial failure reporting.

    Returns a dict: {"batchItemFailures": [{"itemIdentifier": messageId}, ...]}
    """
    corr_id = extract_correlation_id(event)
    log = get_logger(__name__, correlation_id=corr_id) if corr_id else logger

    records: List[Dict[str, Any]] = list(event.get("Records", []))
    failures: List[Dict[str, str]] = []

    for r in records:
        msg_id = r.get("messageId") or r.get("messageID") or "unknown"
        body_raw = r.get("body")
        try:
            payload = json.loads(body_raw) if isinstance(body_raw, str) else (body_raw or {})
        except Exception:
            # Malformed body; mark as failure for retry/DLQ
            failures.append({"itemIdentifier": msg_id})
            continue

        try:
            # Delegate to shared ingestion logic (single call per message)
            result = process_event(payload, context)
            # Treat non-2xx (explicit 4xx/5xx) as failure to enable retry/DLQ
            if isinstance(result, dict):
                status_code = result.get("statusCode")
                if isinstance(status_code, int) and status_code >= 400:
                    failures.append({"itemIdentifier": msg_id})
                    continue
        except Exception:
            # If ingestion fails for this message, signal partial failure for retry
            log.exception("Worker failed to process message")
            failures.append({"itemIdentifier": msg_id})

    return {"batchItemFailures": failures}
