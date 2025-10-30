from __future__ import annotations

import os
from typing import Any, Dict

import boto3


def _client(service: str) -> boto3.client:  # type: ignore[name-defined]
    """Create a LocalStack-scoped boto3 client for the given service."""
    endpoint = os.environ.get("LOCALSTACK_ENDPOINT")
    region = os.environ.get("AWS_REGION", "us-east-1")
    return boto3.client(
        service,
        endpoint_url=endpoint,
        region_name=region,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    """Compaction 결과를 검사해 후속 단계를 진행할지 결정하는 Guard 스텁."""
    preflight = event.get("preflight") or {}
    args = preflight.get("glue_args") or {}

    s3 = _client("s3")
    bucket = args.get("--compacted_bucket")
    domain = args.get("--domain")
    table = args.get("--table_name")
    interval = args.get("--interval")
    data_source = args.get("--data_source")
    ds = args.get("--ds")
    layer = args.get("--compacted_layer")

    prefix = (
        f"{domain}/{table}/interval={interval}/data_source={data_source}/"
        f"year={ds[0:4]}/month={ds[5:7]}/day={ds[8:10]}/layer={layer}/"
    )
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return {"shouldProcess": int(response.get("KeyCount", 0)) > 0, "prefix": prefix}
