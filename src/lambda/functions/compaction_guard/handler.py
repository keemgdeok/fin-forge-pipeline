"""Lambda that validates Glue compaction output before running transforms."""

from __future__ import annotations

import os
from typing import Any, Dict

import boto3
from botocore.exceptions import ClientError


def _normalize_prefix(prefix: str) -> str:
    """Strip leading/trailing slashes from S3 key prefixes."""

    return prefix.strip("/")


def _build_partition_prefix(base_prefix: str, ds: str) -> str:
    """Compose the compacted partition key prefix for the provided date."""

    normalized = _normalize_prefix(base_prefix)
    if normalized:
        return f"{normalized}/ds={ds}/"
    return f"ds={ds}/"


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Return whether compacted parquet output exists for the requested partition."""

    raw_bucket = event.get("bucket")
    bucket = str(raw_bucket).strip() if raw_bucket is not None else ""
    if not bucket:
        raise ValueError("Compaction guard requires an S3 bucket input")

    raw_prefix = event.get("prefix")
    if raw_prefix is None or str(raw_prefix).strip() == "":
        raw_prefix = os.environ.get("DEFAULT_COMPACTED_PREFIX", "")
    prefix = str(raw_prefix).strip()

    raw_ds = event.get("ds")
    ds = str(raw_ds).strip() if raw_ds is not None else ""
    if not ds:
        raise ValueError("Compaction guard requires a ds parameter")

    partition_prefix = _build_partition_prefix(prefix, ds)
    client = boto3.client("s3")

    try:
        response = client.list_objects_v2(Bucket=bucket, Prefix=partition_prefix, MaxKeys=1)
    except ClientError as error:  # pragma: no cover - surfaced to Step Functions fail chain
        raise RuntimeError("Failed to inspect compaction output") from error

    key_count = int(response.get("KeyCount", 0))
    should_process = key_count > 0
    result = {
        "ds": ds,
        "bucket": bucket,
        "partitionPrefix": partition_prefix,
        "objectCount": key_count,
        "shouldProcess": should_process,
        "s3Uri": f"s3://{bucket}/{partition_prefix}",
    }

    if should_process:
        print(f"Compaction guard found {key_count} object(s) at {result['s3Uri']}")
    else:
        print(f"Compaction guard found no objects at {result['s3Uri']}")

    return result
