"""Integration test: run Indicators ETL with Spark local using file:// URIs.

This test is gated by env var RUN_SPARK_TESTS to avoid heavy runs by default.
It uses local filesystem for curated prices/indicators and moto for artifacts S3.
"""

from __future__ import annotations

import os
import runpy
from tempfile import TemporaryDirectory

import boto3
import pandas as pd
import pytest
from moto import mock_aws


RUN_LOCAL = os.environ.get("RUN_SPARK_TESTS", "0") in {"1", "true", "TRUE", "yes"}


pytestmark = pytest.mark.skipif(not RUN_LOCAL, reason="Set RUN_SPARK_TESTS=1 to run Spark-local test")


@pytest.mark.integration
@pytest.mark.glue
@mock_aws
def test_indicators_etl_with_file_scheme() -> None:
    s3 = boto3.client("s3", region_name="us-east-1")
    artifacts_bucket = "test-artifacts-bucket"
    s3.create_bucket(Bucket=artifacts_bucket)

    ds = "2025-09-07"
    with TemporaryDirectory() as tmp:
        # Prepare local curated prices partition
        in_dir = os.path.join(tmp, "market", "prices", "adjusted", f"ds={ds}")
        os.makedirs(in_dir, exist_ok=True)
        df = pd.DataFrame(
            [
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
                },
            ]
        )
        in_file = os.path.join(in_dir, "data.parquet")
        df.to_parquet(in_file, engine="pyarrow", index=False)

        # Build Glue args via sys.argv for the job script
        argv = [
            "script.py",
            "--JOB_NAME",
            "test-indicators-job",
            "--environment",
            "test",
            "--prices_curated_bucket",
            tmp,
            "--prices_prefix",
            "market/prices/adjusted/",
            "--output_bucket",
            tmp,
            "--output_prefix",
            "market/prices/indicators/",
            "--schema_fingerprint_s3_uri",
            f"s3://{artifacts_bucket}/market/prices/indicators/_schema/latest.json",
            "--codec",
            "zstd",
            "--target_file_mb",
            "256",
            "--ds",
            ds,
            "--lookback_days",
            "20",
            "--uri_scheme",
            "file",
        ]

        # Inject argv and run the job module
        import sys as _sys

        prev_argv = list(_sys.argv)
        try:
            _sys.argv = argv
            runpy.run_path("src/glue/jobs/market_indicators_etl.py")
        finally:
            _sys.argv = prev_argv

        # Verify output partition exists
        out_dir = os.path.join(tmp, "market", "prices", "indicators", f"ds={ds}")
        files = []
        for root, _dirs, filenames in os.walk(out_dir):
            for f in filenames:
                files.append(os.path.join(root, f))
        assert any(f.endswith(".parquet") for f in files), "Indicators parquet should be written"

        # Verify fingerprint written to moto S3
        obj = s3.get_object(Bucket=artifacts_bucket, Key="market/prices/indicators/_schema/latest.json")
        assert obj["ResponseMetadata"]["HTTPStatusCode"] == 200
