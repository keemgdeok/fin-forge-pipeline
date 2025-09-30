"""Integration test skeleton for Indicators ETL (documentation-oriented).

This skeleton shows how to prepare curated prices input in S3 and how one might
invoke the indicators Glue job in a real integration test. It is intentionally
lightweight and marked as integration to keep PR checks fast.
"""

from __future__ import annotations

from io import BytesIO
from typing import Dict

import boto3
import pandas as pd
import pytest
from moto import mock_aws

from shared.paths import build_curated_layer_path


@pytest.mark.integration
@mock_aws
def test_indicators_etl_skeleton() -> None:
    s3 = boto3.client("s3", region_name="us-east-1")

    curated_bucket = "test-curated-bucket"
    artifacts_bucket = "test-artifacts-bucket"
    s3.create_bucket(Bucket=curated_bucket)
    s3.create_bucket(Bucket=artifacts_bucket)

    # Prepare minimal curated prices partition for ds=2025-09-07
    ds = "2025-09-07"
    interval = "1d"
    data_source = "yahoo_finance"
    data = [
        {
            "symbol": "AAA",
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "adjusted_close": 100.5,
            "volume": 1000,
            "raw_open": 100.0,
            "raw_high": 101.0,
            "raw_low": 99.0,
            "raw_close": 100.5,
            "raw_volume": 1000,
            "adjustment_factor": 1.0,
            "ds": ds,
            "layer": "adjusted",
        },
        {
            "symbol": "BBB",
            "open": 50.0,
            "high": 51.0,
            "low": 49.0,
            "close": 50.2,
            "adjusted_close": 50.2,
            "volume": 500,
            "raw_open": 50.0,
            "raw_high": 51.0,
            "raw_low": 49.0,
            "raw_close": 50.2,
            "raw_volume": 500,
            "adjustment_factor": 1.0,
            "ds": ds,
            "layer": "adjusted",
        },
    ]
    df = pd.DataFrame(data)
    buf = BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    curated_key = build_curated_layer_path(
        domain="market",
        table="prices",
        interval=interval,
        data_source=data_source,
        ds=ds,
        layer="adjusted",
    )
    s3.put_object(
        Bucket=curated_bucket,
        Key=f"{curated_key}/data.parquet",
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )

    # In a full integration test, you would submit a Glue job with arguments like:
    glue_args: Dict[str, str] = {
        "--environment": "test",
        "--domain": "market",
        "--table_name": "prices",
        "--prices_curated_bucket": curated_bucket,
        "--prices_layer": "adjusted",
        "--output_bucket": curated_bucket,
        "--output_layer": "technical_indicator",
        "--interval": interval,
        "--data_source": data_source,
        "--schema_fingerprint_s3_uri": f"s3://{artifacts_bucket}/market/prices/indicators/_schema/latest.json",
        "--codec": "zstd",
        "--target_file_mb": "256",
        "--ds": ds,
        "--lookback_days": "20",
    }

    # Example placeholder assertion to indicate setup works
    assert glue_args["--prices_layer"] == "adjusted"
    # NOTE: Actual Glue job submission is environment dependent and omitted here.
