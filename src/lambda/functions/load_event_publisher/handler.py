"""EventBridge → SQS transformer for the load pipeline.

Receives curated S3 Object Created events, validates them via shared load contracts,
then publishes normalized messages to the corresponding load queue with required
message attributes for priority/routing.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict
from uuid import uuid4

import boto3

from load_contracts import ValidationError, transform_s3_event_to_message

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

_sqs = boto3.client("sqs")
_QUEUE_MAP: Dict[str, str] = json.loads(os.environ["LOAD_QUEUE_MAP"])
_PRIORITY_MAP: Dict[str, str] = json.loads(os.environ.get("PRIORITY_MAP", "{}"))
_MIN_FILE_SIZE = int(os.environ.get("MIN_FILE_SIZE_BYTES", "1024"))


def _should_process(event: Dict[str, Any]) -> bool:
    size = event.get("detail", {}).get("object", {}).get("size")
    try:
        return int(size) >= _MIN_FILE_SIZE
    except (TypeError, ValueError):
        return False


def _send_message(queue_url: str, body: Dict[str, Any]) -> None:
    attributes = {
        "ContentType": {"DataType": "String", "StringValue": "application/json"},
        "Domain": {"DataType": "String", "StringValue": body["domain"]},
        "TableName": {"DataType": "String", "StringValue": body["table_name"]},
        "Priority": {
            "DataType": "String",
            "StringValue": _PRIORITY_MAP.get(body["domain"], "3"),
        },
    }
    _sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(body), MessageAttributes=attributes)


def main(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    LOGGER.debug("Received event: %s", json.dumps(event))

    if not _should_process(event):
        LOGGER.info("Skipping event due to size threshold")
        return {"status": "SKIPPED", "reason": "File size below threshold"}

    try:
        message = transform_s3_event_to_message(event)
    except ValidationError as exc:
        LOGGER.warning("Dropping event due to validation error: %s", exc)
        return {"status": "SKIPPED", "reason": str(exc)}

    domain = message.get("domain")
    queue_url = _QUEUE_MAP.get(domain)
    if not queue_url:
        LOGGER.warning("No queue configured for domain %s; event dropped", domain)
        return {"status": "SKIPPED", "reason": "Unknown domain"}

    # Ensure correlation id exists even if upstream omitted
    message.setdefault("correlation_id", str(uuid4()))

    _send_message(queue_url, message)
    LOGGER.info("Published load message for domain %s to %s", domain, queue_url)
    return {"status": "SUCCESS", "queue": queue_url}
