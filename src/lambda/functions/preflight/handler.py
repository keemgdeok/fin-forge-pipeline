"""Preflight Lambda for Transform pipeline.

Purpose
- Derive partition information (`interval`, `data_source`, `ds`) from the raw S3
  object key (expects `interval=.../data_source=.../year=YYYY/month=MM/day=DD`
  segments) or accept direct parameters.
- Perform idempotency check by looking for an existing curated partition.
- Return Glue arguments for the transform job.

Error contract (spec-aligned)
- PRE_VALIDATION_FAILED: Missing/invalid inputs
- IDEMPOTENT_SKIP: Already processed (treat as successful short-circuit at SFN)

Input event (S3 -> EventBridge rule)
{
  "source_bucket": "data-pipeline-raw-dev-123456789012",
  "source_key": "market/prices/interval=1d/data_source=yahoo_finance/year=2025/month=09/day=07/AAPL.json",
  "domain": "market",
  "table_name": "prices",
  "file_type": "json"
}

Notes
- Supports both `table_name` and legacy alias `table` for compatibility with docs/tests.

Output
{
  "proceed": true,
  "reason": null,
  "ds": "2025-09-07",
  "glue_args": {"--ds": "2025-09-07", "--interval": "1d", "--data_source": "yahoo_finance", ...}
}
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

# Optional import of typed event models from Common Layer. Fallback to dict-based
# parsing if the layer is not packaged for local tests.
try:  # pragma: no cover
    from shared.models import TransformPreflightEvent

    _HAVE_SHARED_MODELS = True
except Exception:  # pragma: no cover
    _HAVE_SHARED_MODELS = False


_LEGACY_DS_PATTERN = re.compile(r"ingestion_date=(\d{4}-\d{2}-\d{2})")
_DATE_COMPONENT_PATTERN = re.compile(r"year=(\d{4})/month=(\d{2})/day=(\d{2})")
_INTERVAL_PATTERN = re.compile(r"interval=([^/]+)/")
_DATA_SOURCE_PATTERN = re.compile(r"data_source=([^/]+)/")


def _extract_ds_from_key(key: str) -> Optional[str]:
    legacy = _LEGACY_DS_PATTERN.search(key)
    if legacy:
        return legacy.group(1)
    components = _DATE_COMPONENT_PATTERN.search(key)
    if components:
        year, month, day = components.groups()
        return f"{year}-{month}-{day}"
    return None


def _extract_interval_and_source(key: str) -> Tuple[Optional[str], Optional[str]]:
    interval_match = _INTERVAL_PATTERN.search(key)
    data_source_match = _DATA_SOURCE_PATTERN.search(key)
    interval = interval_match.group(1) if interval_match else None
    data_source = data_source_match.group(1) if data_source_match else None
    return interval, data_source


def _curated_partition_exists(s3_client, bucket: str, prefix: str) -> bool:
    try:
        resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        return int(resp.get("KeyCount", 0)) > 0
    except ClientError:
        return False


def _coalesce_table_name(event: Dict[str, Any]) -> str:
    """Return table name from `table_name` or legacy alias `table`.

    This preserves backward compatibility with earlier docs/specs using `table`.
    """
    raw_table_name = event.get("table_name")
    legacy_table = event.get("table")
    # Prefer explicit table_name when provided
    chosen = (raw_table_name or legacy_table or "").strip()
    return chosen


def _normalize_suffixes(values: Any) -> list[str]:
    if not values:
        return []

    if isinstance(values, (list, tuple, set)):
        iterable = values
    else:
        iterable = [values]

    normalized: list[str] = []
    for item in iterable:
        candidate = str(item or "").strip()
        if candidate:
            normalized.append(candidate)
    return normalized


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    parsed_event: Optional["TransformPreflightEvent"] = None
    if _HAVE_SHARED_MODELS:
        try:
            parsed_event = TransformPreflightEvent.model_validate(event)
        except Exception:
            parsed_event = None

    if parsed_event:
        source_bucket = str(parsed_event.source_bucket or "")
        source_key = str(parsed_event.source_key or "")
        domain = str(parsed_event.domain or "").strip()
        table_name = parsed_event.resolved_table_name
        file_type = parsed_event.file_type
        direct_ds = str(parsed_event.ds or "")
        allowed_suffixes = _normalize_suffixes(parsed_event.allowed_suffixes)
        interval_value = str(parsed_event.interval or "").strip()
        data_source_value = str(parsed_event.data_source or "").strip()
    else:
        source_bucket = str(event.get("source_bucket", ""))
        source_key = str(event.get("source_key", ""))
        domain = str(event.get("domain", "")).strip()
        table_name = _coalesce_table_name(event)
        file_type = str(event.get("file_type", "json"))
        direct_ds = str(event.get("ds", ""))
        allowed_suffixes = _normalize_suffixes(event.get("allowed_suffixes"))
        interval_value = str(event.get("interval", "")).strip()
        data_source_value = str(event.get("data_source", "")).strip()

    if not interval_value or not data_source_value:
        derived_interval, derived_source = _extract_interval_and_source(source_key or "")
        if not interval_value and derived_interval:
            interval_value = derived_interval
        if not data_source_value and derived_source:
            data_source_value = derived_source

    if not domain or not table_name or not interval_value or not data_source_value:
        result = {
            "proceed": False,
            "reason": "Missing required fields",
            "error": {
                "code": "PRE_VALIDATION_FAILED",
                "message": "Missing domain/table_name/interval/data_source",
            },
        }
        return result

    # Handle backfill date range: build per-date items with per-item glue_args
    if parsed_event:
        date_range = parsed_event.date_range
    else:
        date_range = event.get("date_range")

    if isinstance(date_range, dict) and date_range.get("start") and date_range.get("end"):
        catalog_update = str(event.get("catalog_update", "")).strip() or None
        start = str(date_range["start"]).strip()
        end = str(date_range["end"]).strip()
        max_days = int(os.environ.get("MAX_BACKFILL_DAYS", "31"))

        # Build date list inclusive, clamped to max_days
        from datetime import datetime, timedelta

        def _to_date(s: str) -> Any:
            return datetime.strptime(s, "%Y-%m-%d")

        d0 = _to_date(start)
        d1 = _to_date(end)
        if d1 < d0:
            d0, d1 = d1, d0
        total_days = (d1 - d0).days + 1
        if total_days > max_days:
            d1 = d0 + timedelta(days=max_days - 1)
            total_days = max_days

        s3 = boto3.client("s3")
        items: list[Dict[str, Any]] = []
        for i in range(total_days):
            ds_i = (d0 + timedelta(days=i)).strftime("%Y-%m-%d")
            curated_prefix = f"{domain}/{table_name}/ds={ds_i}/"
            exists = _curated_partition_exists(
                s3,
                os.environ.get("CURATED_BUCKET", ""),
                curated_prefix,
            )

            artifacts_bucket_env = os.environ.get("ARTIFACTS_BUCKET", "")
            schema_fp_uri_i = f"s3://{artifacts_bucket_env}/{domain}/{table_name}/_schema/latest.json"

            raw_prefix = f"{domain}/{table_name}/interval={interval_value}/" f"data_source={data_source_value}/"

            glue_args_i: Dict[str, str] = {
                "--environment": os.environ.get("ENVIRONMENT", "dev"),
                "--raw_bucket": os.environ.get("RAW_BUCKET", ""),
                "--raw_prefix": raw_prefix,
                "--curated_bucket": os.environ.get("CURATED_BUCKET", ""),
                "--curated_prefix": f"{domain}/{table_name}/",
                "--schema_fingerprint_s3_uri": schema_fp_uri_i,
                "--codec": "zstd",
                "--target_file_mb": "256",
                "--expected_min_records": os.environ.get("EXPECTED_MIN_RECORDS", "100"),
                "--max_critical_error_rate": os.environ.get("MAX_CRITICAL_ERROR_RATE", "5.0"),
                "--ds": ds_i,
                "--file_type": file_type,
                "--interval": interval_value,
                "--data_source": data_source_value,
            }

            if exists:
                items.append(
                    {
                        "proceed": False,
                        "reason": "Already processed",
                        "ds": ds_i,
                        "error": {"code": "IDEMPOTENT_SKIP", "message": "Curated partition already exists"},
                        **({"catalog_update": catalog_update} if catalog_update else {}),
                    }
                )
            else:
                item: Dict[str, Any] = {"proceed": True, "ds": ds_i, "glue_args": glue_args_i}
                if catalog_update:
                    item["catalog_update"] = catalog_update
                items.append(item)

        return {"dates": items}

    if direct_ds:
        ds = direct_ds
    else:
        if not source_bucket or not source_key:
            result = {
                "proceed": False,
                "reason": "Missing required fields",
                "error": {
                    "code": "PRE_VALIDATION_FAILED",
                    "message": "Missing source_bucket/source_key for S3 trigger mode",
                },
            }
            return result

        if allowed_suffixes:
            key_lower = source_key.lower()
            allowed_lower = [s.lower() for s in allowed_suffixes]
            if not any(key_lower.endswith(sfx) for sfx in allowed_lower):
                result = {
                    "proceed": False,
                    "reason": "Object suffix not allowed",
                    "error": {
                        "code": "IGNORED_OBJECT",
                        "message": f"Object key does not match allowed suffixes: {allowed_suffixes}",
                    },
                }
                return result
        extracted_ds = _extract_ds_from_key(source_key or "")
        if not extracted_ds:
            result = {
                "proceed": False,
                "reason": "date partition not found in key",
                "error": {
                    "code": "PRE_VALIDATION_FAILED",
                    "message": "year=YYYY/month=MM/day=DD not found in key",
                },
            }
            return result

        # At this point extracted_ds is guaranteed to be non-None due to the check above
        ds = extracted_ds

    # Env
    curated_bucket = os.environ.get("CURATED_BUCKET", "")
    raw_bucket = os.environ.get("RAW_BUCKET", "")
    artifacts_bucket = os.environ.get("ARTIFACTS_BUCKET", "")
    environment = os.environ.get("ENVIRONMENT", "dev")

    if not curated_bucket or not raw_bucket or not artifacts_bucket:
        result = {
            "proceed": False,
            "reason": "Bucket env not configured",
            "ds": ds,
            "error": {
                "code": "PRE_VALIDATION_FAILED",
                "message": "Missing CURATED_BUCKET/RAW_BUCKET/ARTIFACTS_BUCKET",
            },
        }
        return result

    s3 = boto3.client("s3")

    # Idempotency: skip if curated ds partition already exists
    curated_prefix = f"{domain}/{table_name}/ds={ds}/"
    if _curated_partition_exists(s3, curated_bucket, curated_prefix):
        result = {
            "proceed": False,
            "reason": "Already processed",
            "ds": ds,
            "error": {
                "code": "IDEMPOTENT_SKIP",
                "message": "Curated partition already exists",
            },
        }
        return result

    # Build Glue args (merge with job defaults on StartJobRun)
    schema_fp_uri = f"s3://{artifacts_bucket}/{domain}/{table_name}/_schema/latest.json"
    raw_prefix = f"{domain}/{table_name}/interval={interval_value}/" f"data_source={data_source_value}/"

    glue_args: Dict[str, str] = {
        "--environment": environment,
        "--raw_bucket": raw_bucket,
        "--raw_prefix": raw_prefix,
        "--curated_bucket": curated_bucket,
        "--curated_prefix": f"{domain}/{table_name}/",
        # Artifacts path aligned to spec: <domain>/<table>/_schema/latest.json
        "--schema_fingerprint_s3_uri": schema_fp_uri,
        "--codec": "zstd",
        "--target_file_mb": "256",
        "--expected_min_records": os.environ.get("EXPECTED_MIN_RECORDS", "100"),
        "--max_critical_error_rate": os.environ.get("MAX_CRITICAL_ERROR_RATE", "5.0"),
        "--ds": ds,
        "--file_type": file_type,
        "--interval": interval_value,
        "--data_source": data_source_value,
    }

    result = {
        "proceed": True,
        "reason": None,
        "ds": ds,
        "glue_args": glue_args,
    }
    catalog_update = str(event.get("catalog_update", "")).strip()
    if catalog_update:
        result["catalog_update"] = catalog_update
    return result
