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
    """Copy RAW 객체를 compacted 레이어로 이동하는 스텁 구현."""
    preflight = event.get("preflight") or {}
    args = preflight.get("compaction_args") or {}
    if not args:
        return {"copied": 0}

    s3 = _client("s3")
    raw_bucket = args["--raw_bucket"]
    raw_prefix = args["--raw_prefix"].rstrip("/") + "/"
    compacted_bucket = args["--compacted_bucket"]
    layer = args["--layer"]
    domain = args["--domain"]
    table = args["--table_name"]
    interval = args["--interval"]
    data_source = args["--data_source"]
    ds = args["--ds"]

    target_prefix = (
        f"{domain}/{table}/interval={interval}/data_source={data_source}/"
        f"year={ds[0:4]}/month={ds[5:7]}/day={ds[8:10]}/layer={layer}"
    )

    copied = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=raw_bucket, Prefix=raw_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            body = s3.get_object(Bucket=raw_bucket, Key=key)["Body"].read()
            s3.put_object(
                Bucket=compacted_bucket,
                Key=f"{target_prefix}/{key.split('/')[-1]}",
                Body=body,
                ContentType="application/json",
            )
            copied += 1

    return {"copied": copied, "compacted_prefix": target_prefix}
