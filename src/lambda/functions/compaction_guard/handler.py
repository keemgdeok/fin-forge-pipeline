"""Lambda that validates Glue compaction output before running transforms."""

from __future__ import annotations

from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError

from shared.paths import build_curated_layer_path


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Return whether compacted parquet output exists for the requested partition."""

    bucket_raw = event.get("bucket")
    bucket = str(bucket_raw or "").strip()
    if not bucket:
        raise ValueError("Compaction guard requires an S3 bucket input")

    domain = str(event.get("domain") or "").strip()
    table_name = str(event.get("table_name") or "").strip()
    interval = str(event.get("interval") or "").strip()
    if not domain or not table_name or not interval:
        raise ValueError("Compaction guard requires domain, table_name, and interval")

    data_source_raw: Optional[str] = event.get("data_source")
    data_source = str(data_source_raw or "").strip() or None

    layer = str(event.get("layer") or "").strip()
    if not layer:
        raise ValueError("Compaction guard requires a layer parameter")

    ds_raw = event.get("ds")
    ds = str(ds_raw or "").strip()
    if not ds:
        raise ValueError("Compaction guard requires a ds parameter")

    partition_prefix = build_curated_layer_path(
        domain=domain,
        table=table_name,
        interval=interval,
        data_source=data_source,
        ds=ds,
        layer=layer,
    )
    search_prefix = f"{partition_prefix.rstrip('/')}/"

    client = boto3.client("s3")

    try:
        response = client.list_objects_v2(Bucket=bucket, Prefix=search_prefix, MaxKeys=1)
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
