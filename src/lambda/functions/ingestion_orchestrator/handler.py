"""Orchestrator Lambda: fans out symbol universe into SQS messages.

Reads a scheduled EventBridge event containing ingestion parameters
(domain, table_name, symbols, period, interval, file_format) and
publishes chunked messages to the target SQS queue.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

from shared.utils.logger import get_logger, extract_correlation_id


logger = get_logger(__name__)


def _chunks(items: List[str], size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _send_batch_with_retry(
    sqs_client: Any,
    queue_url: str,
    entries: List[Dict[str, Any]],
    log: Any,
    *,
    max_retry: int = 1,
) -> int:
    """Send SQS batch, log partial failures, retry failed entries once, and raise on residual failures.

    Returns number of successfully published entries.
    """
    published = 0
    id_to_entry = {e.get("Id"): e for e in entries}

    try:
        resp = sqs_client.send_message_batch(QueueUrl=queue_url, Entries=entries)
    except ClientError:
        log.exception("Failed to send SQS batch (ClientError)")
        raise
    except Exception:
        log.exception("Failed to send SQS batch (unexpected)")
        raise

    published += len(resp.get("Successful", []))
    failed = list(resp.get("Failed", []))
    if not failed:
        return published

    # Log failed entries
    log.warning(
        "SQS batch returned failed entries",
        extra={
            "failed_count": len(failed),
            "failed_ids": [f.get("Id") for f in failed],
            "failed_codes": [f.get("Code") for f in failed],
        },
    )

    # Retry only failed entries up to max_retry times
    failed_entries = [id_to_entry[i] for i in [f.get("Id") for f in failed] if i in id_to_entry]
    attempts = 0
    while failed_entries and attempts < max_retry:
        attempts += 1
        try:
            resp2 = sqs_client.send_message_batch(QueueUrl=queue_url, Entries=failed_entries)
        except ClientError:
            log.exception("Retry send SQS batch failed (ClientError)")
            raise
        except Exception:
            log.exception("Retry send SQS batch failed (unexpected)")
            raise

        published += len(resp2.get("Successful", []))
        failed = list(resp2.get("Failed", []))
        failed_entries = [id_to_entry[i] for i in [f.get("Id") for f in failed] if i in id_to_entry]
        if failed:
            log.warning(
                "SQS batch retry still has failed entries",
                extra={
                    "failed_count": len(failed),
                    "failed_ids": [f.get("Id") for f in failed],
                    "failed_codes": [f.get("Code") for f in failed],
                    "attempts": attempts,
                },
            )

    if failed_entries:
        # Residual failures after retry: raise to surface problem to scheduler/retry
        raise RuntimeError("SQS batch had failed entries after retry")

    return published


def _load_symbols_from_sources(
    ssm_param: Optional[str], s3_bucket: Optional[str], s3_key: Optional[str]
) -> Optional[List[str]]:
    """Attempt to load symbol universe from SSM or S3.

    Format supported:
      - JSON array of strings
      - Newline-separated text
    Returns None if nothing could be loaded.
    """
    # Try SSM first if provided
    if ssm_param:
        try:
            ssm = boto3.client("ssm")
            resp = ssm.get_parameter(Name=ssm_param, WithDecryption=False)
            value: str = resp["Parameter"]["Value"]
            syms = _parse_symbol_text(value)
            if syms:
                return syms
        except ClientError:
            pass

    # Fallback to S3
    if s3_bucket and s3_key:
        try:
            s3 = boto3.client("s3")
            obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)
            text = obj["Body"].read().decode("utf-8")
            syms = _parse_symbol_text(text)
            if syms:
                return syms
        except ClientError:
            pass
    return None


def _parse_symbol_text(text: str) -> List[str]:
    try:
        data = json.loads(text)
        if isinstance(data, list):
            return [str(s).strip() for s in data if isinstance(s, (str, int)) and str(s).strip()]
    except Exception:
        pass
    # Treat as newline separated
    return [line.strip() for line in text.splitlines() if line.strip()]


def _init_batch_tracker_entry(
    *,
    table_name: str,
    batch_id: str,
    batch_ds: str,
    expected_chunks: int,
    metadata: Dict[str, Any],
    ttl_days: int,
    log: Any,
) -> None:
    if not table_name or expected_chunks <= 0:
        return

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)
    now = datetime.now(timezone.utc)

    item: Dict[str, Any] = {
        "pk": batch_id,
        "batch_ds": batch_ds,
        "expected_chunks": Decimal(expected_chunks),
        "processed_chunks": Decimal(0),
        "status": "processing",
        "created_at": now.isoformat(),
        "last_update": now.isoformat(),
        **{f"meta_{k}": v for k, v in metadata.items()},
    }

    if ttl_days > 0:
        item["ttl"] = int((now + timedelta(days=ttl_days)).timestamp())

    try:
        table.put_item(Item=item, ConditionExpression="attribute_not_exists(pk)")
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            log.warning(
                "Batch tracker entry already exists",
                extra={"batch_id": batch_id},
            )
        else:
            log.exception("Failed to initialize batch tracker entry")
            raise


def main(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Entry point for the orchestrator Lambda.

    Args:
        event: Expected to include domain, table_name, symbols(list[str]), period, interval, file_format
        context: Lambda context (unused)

    Returns: Summary of published batches
    """
    corr_id = extract_correlation_id(event)
    log = get_logger(__name__, correlation_id=corr_id) if corr_id else logger

    env = os.environ.get("ENVIRONMENT", "dev")
    queue_url = os.environ.get("QUEUE_URL")
    chunk_size = max(1, int(os.environ.get("CHUNK_SIZE", "5")))
    send_batch_size = max(1, min(10, int(os.environ.get("SQS_SEND_BATCH_SIZE", "10"))))
    batch_table = os.environ.get("BATCH_TRACKING_TABLE", "")
    ttl_days = int(os.environ.get("BATCH_TRACKER_TTL_DAYS", "7"))

    if not queue_url:
        raise ValueError("QUEUE_URL environment variable is required")

    # Extract parameters from event with safe fallbacks
    # Load symbol universe from SSM or S3 if configured; else from event
    symbols = [s for s in (event.get("symbols") or []) if isinstance(s, str) and s.strip()]
    ssm_param = os.environ.get("SYMBOLS_SSM_PARAM")
    sym_s3_bucket = os.environ.get("SYMBOLS_S3_BUCKET")
    sym_s3_key = os.environ.get("SYMBOLS_S3_KEY")

    loaded = _load_symbols_from_sources(ssm_param, sym_s3_bucket, sym_s3_key)
    if loaded:
        symbols = loaded
    if not symbols:
        # fallback to a very small default to keep pipeline alive in dev
        symbols = ["AAPL"]

    params = {
        "data_source": event.get("data_source", "yahoo_finance"),
        "data_type": event.get("data_type", "prices"),
        "domain": event.get("domain", "market"),
        "table_name": event.get("table_name", "prices"),
        "period": event.get("period", "1mo"),
        "interval": event.get("interval", "1d"),
        "file_format": event.get("file_format", "json"),
    }

    total_symbols = len(symbols)
    chunks_count = (total_symbols + chunk_size - 1) // chunk_size if total_symbols else 0

    batch_ds = str(event.get("batch_ds") or event.get("ds") or datetime.now(timezone.utc).date().isoformat())
    batch_id = str(event.get("batch_id") or uuid.uuid4())

    if batch_table and chunks_count > 0:
        _init_batch_tracker_entry(
            table_name=batch_table,
            batch_id=batch_id,
            batch_ds=batch_ds,
            expected_chunks=chunks_count,
            metadata={
                "environment": env,
                "domain": params["domain"],
                "table_name": params["table_name"],
                "interval": params["interval"],
                "data_source": params["data_source"],
            },
            ttl_days=ttl_days,
            log=log,
        )

    sqs = boto3.client("sqs")

    published = 0
    entries: List[Dict[str, Any]] = []
    msg_id_counter = 0
    for ch in _chunks(symbols, chunk_size):
        body = {
            **params,
            "symbols": ch,
            "batch_id": batch_id,
            "batch_ds": batch_ds,
            "batch_total_chunks": chunks_count,
        }
        payload = json.dumps(body, ensure_ascii=False)
        entries.append({"Id": f"m-{msg_id_counter}", "MessageBody": payload})
        msg_id_counter += 1
        if len(entries) >= send_batch_size:
            published += _send_batch_with_retry(sqs, queue_url, entries, log, max_retry=1)
            entries = []

    if entries:
        published += _send_batch_with_retry(sqs, queue_url, entries, log, max_retry=1)

    log.info(
        "Orchestrated symbols into SQS",
        extra={
            "environment": env,
            "chunks": chunks_count,
            "published": published,
            "batch_id": batch_id,
            "batch_ds": batch_ds,
        },
    )
    return {
        "published": published,
        "chunks": chunks_count,
        "environment": env,
        "batch_id": batch_id,
        "batch_ds": batch_ds,
    }
