"""Preflight Lambda for Transform pipeline.

Derives `ds` from the raw S3 object key (expects `ingestion_date=YYYY-MM-DD` in key),
performs idempotency check by looking for an existing curated partition, and
returns Glue arguments for the transform job.

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
    # Required inputs
    source_bucket = str(event.get("source_bucket", ""))
    source_key = str(event.get("source_key", ""))
    domain = str(event.get("domain", ""))
    table_name = str(event.get("table_name", ""))
    file_type = str(event.get("file_type", "json"))

    if not source_bucket or not source_key or not domain or not table_name:
        return {
            "proceed": False,
            "reason": "Missing required fields",
        }

    ds = _extract_ds_from_key(source_key)
    if not ds:
        return {"proceed": False, "reason": "ingestion_date not found in key"}

    # Env
    curated_bucket = os.environ.get("CURATED_BUCKET", "")
    raw_bucket = os.environ.get("RAW_BUCKET", "")
    artifacts_bucket = os.environ.get("ARTIFACTS_BUCKET", "")
    environment = os.environ.get("ENVIRONMENT", "dev")

    if not curated_bucket or not raw_bucket or not artifacts_bucket:
        return {"proceed": False, "reason": "Bucket env not configured"}

    s3 = boto3.client("s3")

    # Idempotency: skip if curated ds partition already exists
    curated_prefix = f"{domain}/{table_name}/ds={ds}/"
    if _curated_partition_exists(s3, curated_bucket, curated_prefix):
        return {"proceed": False, "reason": "Already processed", "ds": ds}

    # Build Glue args (merge with job defaults on StartJobRun)
    glue_args: Dict[str, str] = {
        "--environment": environment,
        "--raw_bucket": raw_bucket,
        "--raw_prefix": f"{domain}/{table_name}/",
        "--curated_bucket": curated_bucket,
        "--curated_prefix": f"{domain}/{table_name}/",
        "--schema_fingerprint_s3_uri": f"s3://{artifacts_bucket}/schemas/{domain}/{table_name}/latest.json",
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
