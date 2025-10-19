from __future__ import annotations

import json
import os
import uuid
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
    """Generate curated/summary/schema 산출물을 작성하는 ETL 스텁."""
    preflight = event.get("preflight") or {}
    args = preflight.get("glue_args") or {}

    s3 = _client("s3")
    curated_bucket = args["--curated_bucket"]
    artifacts_bucket = event["artifacts_bucket"]
    domain = args["--domain"]
    table = args["--table_name"]
    interval = args["--interval"]
    data_source = args["--data_source"]
    ds = args["--ds"]
    curated_layer = args["--curated_layer"]

    prefix = (
        f"{domain}/{table}/interval={interval}/data_source={data_source}/year={ds[0:4]}/month={ds[5:7]}/day={ds[8:10]}"
    )
    curated_prefix = f"{prefix}/layer={curated_layer}"
    parquet_key = f"{curated_prefix}/part-0000.parquet"

    s3.put_object(
        Bucket=curated_bucket,
        Key=parquet_key,
        Body=b"X" * 2048,
        ContentType="application/octet-stream",
    )

    summary = {
        "domain": domain,
        "table_name": table,
        "ds": ds,
        "interval": interval,
        "data_source": data_source,
        "records": 1,
    }
    summary_key = f"{curated_prefix}/dataset.json"
    s3.put_object(
        Bucket=curated_bucket,
        Key=summary_key,
        Body=json.dumps(summary).encode("utf-8"),
        ContentType="application/json",
    )

    schema_key = f"{domain}/{table}/_schema/latest.json"
    schema_payload = {"hash": str(uuid.uuid4()), "columns": ["symbol", "price", "timestamp"]}
    s3.put_object(
        Bucket=artifacts_bucket,
        Key=schema_key,
        Body=json.dumps(schema_payload).encode("utf-8"),
        ContentType="application/json",
    )

    return {
        "curated_key": parquet_key,
        "summary_key": summary_key,
        "schema_key": schema_key,
    }
