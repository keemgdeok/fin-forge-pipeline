"""SQS Worker Lambda: processes one or few symbols and writes RAW to S3.

Consumes SQS event with records where each body contains the same
payload expected by data_ingestion.handler.main (domain, table_name,
symbols, period, interval, file_format, etc.).

Implements partial batch failure semantics.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Tuple, cast

import boto3
from botocore.exceptions import ClientError

from shared.ingestion.manifests import (
    cleanup_chunk_summaries as _cleanup_chunk_summaries,
    load_chunk_summaries as _load_chunk_summaries,
    persist_chunk_summary as _persist_chunk_summary,
)
from market_shared.ingestion.service import process_event
from shared.utils.logger import get_logger, extract_correlation_id


logger = get_logger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        if value % 1 == 0:
            return int(value)
        return float(value)
    if isinstance(value, list):
        return [_to_json_safe(v) for v in value]
    if isinstance(value, dict):
        return {k: _to_json_safe(v) for k, v in value.items()}
    return value


def _update_batch_tracker(
    *,
    table_name: str,
    batch_id: str,
    batch_ds: str,
    partition_summaries: List[Dict[str, Any]],
    payload: Dict[str, Any],
    log: Any,
) -> Tuple[bool, Dict[str, Any]]:
    """Increment processed chunk counter and return (should_finalize, tracker_attributes)."""

    if not table_name or not batch_id:
        return False, {}

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)
    now = _now_iso()

    expression_values: Dict[str, Any] = {
        ":inc": Decimal(1),
        ":now": now,
        ":ds": batch_ds,
    }
    update_expression = "ADD processed_chunks :inc SET last_update = :now, last_ds = :ds"

    try:
        resp = table.update_item(
            Key={"pk": batch_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_values,
            ConditionExpression="attribute_exists(pk)",
            ReturnValues="ALL_NEW",
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            log.error("Batch tracker entry missing", extra={"batch_id": batch_id})
            raise
        raise

    attrs = resp.get("Attributes", {})
    expected = int(attrs.get("expected_chunks", 0))
    processed = int(attrs.get("processed_chunks", 0))
    status = attrs.get("status", "processing")

    if processed == expected and status == "processing":
        # Attempt to claim finalization
        summary_payload = {
            # Partition summaries are derived from S3 chunk summaries during finalization.
            "partition_summaries": [],
            "symbols": payload.get("symbols", []),
        }
        try:
            table.update_item(
                Key={"pk": batch_id},
                UpdateExpression="SET #status = :finalizing, finalizing_at = :now, finalizing_payload = :summary",
                ExpressionAttributeValues={
                    ":finalizing": "finalizing",
                    ":now": now,
                    ":summary": summary_payload,
                    ":processing": "processing",
                },
                ExpressionAttributeNames={"#status": "status"},
                ConditionExpression="#status = :processing",
            )
            return True, attrs
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                # Another worker already claimed finalization
                return False, attrs
            raise

    return False, attrs


def _write_manifest_for_partition(
    *,
    raw_bucket: str,
    domain: str,
    table_name: str,
    data_source: str,
    interval: str,
    environment: str,
    batch_id: str,
    manifest_basename: str,
    manifest_suffix: str,
    summary: Dict[str, Any],
) -> str:
    ds = str(summary.get("ds"))
    if not ds:
        raise ValueError("partition summary missing ds")

    if manifest_suffix and not manifest_suffix.startswith("."):
        manifest_suffix = f".{manifest_suffix}"

    try:
        year, month, day = ds.split("-")
    except ValueError as exc:
        raise ValueError(f"Invalid ds value for manifest: {ds}") from exc

    manifest_key = (
        f"{domain}/{table_name}/interval={interval}/"
        f"data_source={data_source}/year={year}/month={month}/day={day}/"
        f"{manifest_basename or '_batch'}{manifest_suffix or '.manifest.json'}"
    )

    manifest_body = {
        "environment": environment,
        "domain": domain,
        "table_name": table_name,
        "data_source": data_source,
        "interval": interval,
        "ds": ds,
        "generated_at": _now_iso(),
        "objects": summary.get("objects", []),
        "batch_id": batch_id,
    }

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=raw_bucket,
        Key=manifest_key,
        Body=json.dumps(_to_json_safe(manifest_body), ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    return manifest_key


def _mark_batch_status(table_name: str, batch_id: str, status: str, **extra: Any) -> None:
    if not table_name or not batch_id:
        return

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)
    update_expression = "SET #status = :status, last_update = :now"
    expression_values: Dict[str, Any] = {
        ":status": status,
        ":now": _now_iso(),
    }
    expression_names: Dict[str, str] = {"#status": "status"}

    for idx, (key, value) in enumerate(extra.items(), start=0):
        value = _to_json_safe(value)
        placeholder = f"#k{idx}"
        value_placeholder = f":v{idx}"
        update_expression += f", {placeholder} = {value_placeholder}"
        expression_values[value_placeholder] = value
        expression_names[placeholder] = key

    kwargs: Dict[str, Any] = {
        "Key": {"pk": batch_id},
        "UpdateExpression": update_expression,
        "ExpressionAttributeValues": expression_values,
    }
    if expression_names:
        kwargs["ExpressionAttributeNames"] = expression_names

    table.update_item(**kwargs)


def _emit_manifests(
    *,
    raw_bucket: str,
    manifest_basename: str,
    manifest_suffix: str,
    environment: str,
    batch_id: str,
    payload: Dict[str, Any],
    tracker_attrs: Dict[str, Any],
    partition_entries: List[Dict[str, Any]],
    log: Any,
) -> List[str]:
    combined, chunk_keys = _load_chunk_summaries(
        raw_bucket=raw_bucket,
        batch_id=batch_id,
        log=log,
    )

    if not combined:
        combined_raw = tracker_attrs.get("finalizing_payload", {}).get("partition_summaries")
        if combined_raw is not None:
            combined = cast(List[Dict[str, Any]], combined_raw)
        elif partition_entries:
            combined = [
                {
                    "ds": entry.get("ds"),
                    "objects": [],
                    "raw_prefix": entry.get("raw_prefix", ""),
                }
                for entry in partition_entries
                if entry.get("ds")
            ]

    if not combined or not raw_bucket:
        return []

    domain = str(payload.get("domain", tracker_attrs.get("meta_domain", "market")))
    table_name = str(payload.get("table_name", tracker_attrs.get("meta_table_name", "prices")))
    data_source = str(payload.get("data_source", tracker_attrs.get("meta_data_source", "yahoo_finance")))
    interval = str(payload.get("interval", tracker_attrs.get("meta_interval", "1d")))

    manifest_keys: List[str] = []
    for summary in combined:
        objects = summary.get("objects")
        if not objects:
            raw_prefix = summary.get("raw_prefix", "")
            if raw_prefix:
                objects = []
                s3 = boto3.client("s3")
                paginator = s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=raw_bucket, Prefix=raw_prefix):
                    for obj in page.get("Contents", []):
                        key = obj.get("Key")
                        if key:
                            objects.append({"key": key})

        manifest_summary = {
            "ds": summary.get("ds"),
            "objects": objects or [],
        }
        manifest_key = _write_manifest_for_partition(
            raw_bucket=raw_bucket,
            domain=domain,
            table_name=table_name,
            data_source=data_source,
            interval=interval,
            environment=environment,
            batch_id=batch_id or tracker_attrs.get("pk", ""),
            manifest_basename=manifest_basename,
            manifest_suffix=manifest_suffix,
            summary=manifest_summary,
        )
        manifest_keys.append(manifest_key)

    log.info(
        "Emitted manifests",
        extra={"batch_id": batch_id, "manifest_keys": manifest_keys},
    )
    _cleanup_chunk_summaries(raw_bucket=raw_bucket, keys=chunk_keys, log=log)
    return manifest_keys


def main(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Entry point for SQS batch processing with partial failure reporting.

    Returns a dict: {"batchItemFailures": [{"itemIdentifier": messageId}, ...]}
    """
    corr_id = extract_correlation_id(event)
    log = get_logger(__name__, correlation_id=corr_id) if corr_id else logger

    records: List[Dict[str, Any]] = list(event.get("Records", []))
    failures: List[Dict[str, str]] = []

    batch_table = os.environ.get("BATCH_TRACKING_TABLE", "")
    manifest_basename = (os.environ.get("RAW_MANIFEST_BASENAME") or "_batch").strip() or "_batch"
    manifest_suffix = (os.environ.get("RAW_MANIFEST_SUFFIX") or ".manifest.json").strip()
    raw_bucket = os.environ.get("RAW_BUCKET", "")
    environment = os.environ.get("ENVIRONMENT", "dev")

    for r in records:
        msg_id = r.get("messageId") or r.get("messageID") or "unknown"
        body_raw = r.get("body")
        try:
            payload = json.loads(body_raw) if isinstance(body_raw, str) else (body_raw or {})
        except Exception:
            failures.append({"itemIdentifier": msg_id})
            continue

        try:
            result = process_event(payload, context)
        except Exception:
            log.exception("Worker failed to process message")
            failures.append({"itemIdentifier": msg_id})
            continue

        status_code = result.get("statusCode") if isinstance(result, dict) else None
        if isinstance(status_code, int) and status_code >= 400:
            failures.append({"itemIdentifier": msg_id})
            continue

        body = result.get("body", {}) if isinstance(result, dict) else {}

        partition_summaries: List[Dict[str, Any]] = list(body.get("partition_summaries", []))
        partition_entries_reduced: List[Dict[str, Any]] = [
            {
                "ds": summary.get("ds"),
                "raw_prefix": summary.get("raw_prefix"),
                "object_count": len(summary.get("objects", [])),
            }
            for summary in partition_summaries
            if summary.get("ds")
        ]
        batch_id = str(payload.get("batch_id") or "")
        batch_ds = str(payload.get("batch_ds") or body.get("ds") or "")

        if partition_summaries:
            try:
                _persist_chunk_summary(
                    raw_bucket=raw_bucket,
                    batch_id=batch_id,
                    partition_summaries=partition_summaries,
                    log=log,
                )
            except Exception:
                log.exception(
                    "Failed to store chunk summary",
                    extra={"batch_id": batch_id},
                )

        tracker_attrs: Dict[str, Any] = {}
        should_finalize = False

        if batch_table and batch_id:
            try:
                should_finalize, tracker_attrs = _update_batch_tracker(
                    table_name=batch_table,
                    batch_id=batch_id,
                    batch_ds=batch_ds,
                    partition_summaries=partition_summaries,
                    payload=payload,
                    log=log,
                )
            except Exception:
                log.exception("Failed to update batch tracker")
                failures.append({"itemIdentifier": msg_id})
                continue
        else:
            should_finalize = True

        if not raw_bucket or not should_finalize:
            continue

        try:
            manifest_keys = _emit_manifests(
                raw_bucket=raw_bucket,
                manifest_basename=manifest_basename,
                manifest_suffix=manifest_suffix,
                environment=environment,
                batch_id=batch_id,
                payload=payload,
                tracker_attrs=tracker_attrs,
                partition_entries=partition_entries_reduced,
                log=log,
            )

            if batch_table and batch_id:
                _mark_batch_status(
                    batch_table,
                    batch_id,
                    "complete",
                    manifest_keys=manifest_keys,
                    completed_at=_now_iso(),
                )
        except Exception as exc:
            if batch_table and batch_id:
                _mark_batch_status(
                    batch_table,
                    batch_id,
                    "error",
                    error_message=str(exc),
                )
            log.exception("Failed to finalize batch")
            failures.append({"itemIdentifier": msg_id})

    return {"batchItemFailures": failures}
