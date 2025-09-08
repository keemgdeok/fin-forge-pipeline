"""Preflight Lambda for Transform pipeline.

Derives partition `ds` from the raw S3 object key (expects
`ingestion_date=YYYY-MM-DD` in key), performs idempotency check by looking for an
existing curated partition, and returns Glue arguments for the transform job.

Error contract (spec-aligned):
- PRE_VALIDATION_FAILED: Missing/invalid inputs
- IDEMPOTENT_SKIP: Already processed (treat as successful short-circuit at SFN)

Input event example (from S3->EventBridge rule):
{
  "source_bucket": "data-pipeline-raw-dev-123456789012",
  "source_key": "market/prices/ingestion_date=2025-09-07/file.json",
  "domain": "market",
  "table_name": "prices",
  "file_type": "json"
}

Output example:
{
  "proceed": true,
  "reason": null,
  "ds": "2025-09-07",
  "glue_args": {"--ds": "2025-09-07", ...}
}
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError


def _extract_ds_from_key(key: str) -> Optional[str]:
    m = re.search(r"ingestion_date=(\d{4}-\d{2}-\d{2})", key)
    return m.group(1) if m else None


def _curated_partition_exists(s3_client, bucket: str, prefix: str) -> bool:
    try:
        resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        return int(resp.get("KeyCount", 0)) > 0
    except ClientError:
        return False


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    # Inputs: support two modes per spec: direct ds or S3-trigger
    source_bucket = str(event.get("source_bucket", ""))
    source_key = str(event.get("source_key", ""))
    domain = str(event.get("domain", ""))
    table_name = str(event.get("table_name", ""))
    file_type = str(event.get("file_type", "json"))
    direct_ds = str(event.get("ds", ""))

    if not domain or not table_name:
        return {
            "proceed": False,
            "reason": "Missing required fields",
            "error": {
                "code": "PRE_VALIDATION_FAILED",
                "message": "Missing domain/table_name",
            },
        }

    if direct_ds:
        ds = direct_ds
    else:
        if not source_bucket or not source_key:
            return {
                "proceed": False,
                "reason": "Missing required fields",
                "error": {
                    "code": "PRE_VALIDATION_FAILED",
                    "message": "Missing source_bucket/source_key for S3 trigger mode",
                },
            }
        extracted_ds = _extract_ds_from_key(source_key or "")
        if not extracted_ds:
            return {
                "proceed": False,
                "reason": "ingestion_date not found in key",
                "error": {
                    "code": "PRE_VALIDATION_FAILED",
                    "message": "ingestion_date=YYYY-MM-DD not found in key",
                },
            }

        # At this point extracted_ds is guaranteed to be non-None due to the check above
        ds = extracted_ds

    # Env
    curated_bucket = os.environ.get("CURATED_BUCKET", "")
    raw_bucket = os.environ.get("RAW_BUCKET", "")
    artifacts_bucket = os.environ.get("ARTIFACTS_BUCKET", "")
    environment = os.environ.get("ENVIRONMENT", "dev")

    if not curated_bucket or not raw_bucket or not artifacts_bucket:
        return {
            "proceed": False,
            "reason": "Bucket env not configured",
            "ds": ds,
            "error": {
                "code": "PRE_VALIDATION_FAILED",
                "message": "Missing CURATED_BUCKET/RAW_BUCKET/ARTIFACTS_BUCKET",
            },
        }

    s3 = boto3.client("s3")

    # Idempotency: skip if curated ds partition already exists
    curated_prefix = f"{domain}/{table_name}/ds={ds}/"
    if _curated_partition_exists(s3, curated_bucket, curated_prefix):
        return {
            "proceed": False,
            "reason": "Already processed",
            "ds": ds,
            "error": {
                "code": "IDEMPOTENT_SKIP",
                "message": "Curated partition already exists",
            },
        }

    # Build Glue args (merge with job defaults on StartJobRun)
    glue_args: Dict[str, str] = {
        "--environment": environment,
        "--raw_bucket": raw_bucket,
        "--raw_prefix": f"{domain}/{table_name}/",
        "--curated_bucket": curated_bucket,
        "--curated_prefix": f"{domain}/{table_name}/",
        # Artifacts path aligned to spec: <domain>/<table>/_schema/latest.json
        "--schema_fingerprint_s3_uri": f"s3://{artifacts_bucket}/{domain}/{table_name}/_schema/latest.json",
        "--codec": "zstd",
        "--target_file_mb": "256",
        "--ds": ds,
        "--file_type": file_type,
    }

    return {
        "proceed": True,
        "reason": None,
        "ds": ds,
        "glue_args": glue_args,
    }
