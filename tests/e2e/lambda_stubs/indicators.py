from __future__ import annotations

import json
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
    """인디케이터 산출물을 작성하는 스텁."""
    preflight = event.get("preflight") or {}
    args = preflight.get("glue_args") or {}

    s3 = _client("s3")
    curated_bucket = args["--curated_bucket"]
    domain = args["--domain"]
    table = args["--table_name"]
    interval = args["--interval"]
    data_source = args["--data_source"]
    ds = args["--ds"]
    indicator_layer = event.get("indicator_layer", "technical_indicator")

    prefix = (
        f"{domain}/{table}/interval={interval}/data_source={data_source}/year={ds[0:4]}/month={ds[5:7]}/day={ds[8:10]}"
    )
    indicator_prefix = f"{prefix}/layer={indicator_layer}"
    indicator_key = f"{indicator_prefix}/indicators.json"

    payload = {
        "ds": ds,
        "domain": domain,
        "table_name": table,
        "interval": interval,
        "data_source": data_source,
    }
    s3.put_object(
        Bucket=curated_bucket,
        Key=indicator_key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )
    return {"indicator_key": indicator_key}
