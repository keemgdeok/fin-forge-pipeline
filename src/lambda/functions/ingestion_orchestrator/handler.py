"""Orchestrator Lambda: fans out symbol universe into SQS messages.

Reads a scheduled EventBridge event containing ingestion parameters
(domain, table_name, symbols, period, interval, file_format) and
publishes chunked messages to the target SQS queue.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

from shared.utils.logger import get_logger, extract_correlation_id


logger = get_logger(__name__)


def _chunks(items: List[str], size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


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

    sqs = boto3.client("sqs")

    published = 0
    entries: List[Dict[str, Any]] = []
    msg_id_counter = 0
    chunks_count = 0
    for ch in _chunks(symbols, chunk_size):
        chunks_count += 1
        body = {**params, "symbols": ch}
        payload = json.dumps(body, ensure_ascii=False)
        entries.append({"Id": f"m-{msg_id_counter}", "MessageBody": payload})
        msg_id_counter += 1
        if len(entries) >= send_batch_size:
            sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
            published += len(entries)
            entries = []

    if entries:
        sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
        published += len(entries)

    log.info(
        "Orchestrated symbols into SQS", extra={"environment": env, "chunks": chunks_count, "published": published}
    )
    return {"published": published, "chunks": chunks_count, "environment": env}
