"""Integration test: run Indicators ETL with Spark local using file:// URIs.

This test is gated by env var RUN_SPARK_TESTS to avoid heavy runs by default.
It uses local filesystem for curated prices/indicators and moto for artifacts S3.
"""

from __future__ import annotations

import os
import runpy
import sys
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType

import boto3
import pandas as pd
import pytest
from moto import mock_aws


RUN_LOCAL = os.environ.get("RUN_SPARK_TESTS", "0") in {"1", "true", "TRUE", "yes"}

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_SRC_DIR = _PROJECT_ROOT / "src"
if str(_SRC_DIR) not in sys.path:
    sys.path.append(str(_SRC_DIR))

current_pythonpath = os.environ.get("PYTHONPATH", "")
path_entries = {entry for entry in current_pythonpath.split(os.pathsep) if entry}
if str(_SRC_DIR) not in path_entries:
    updated = os.pathsep.join([str(_SRC_DIR), current_pythonpath]) if current_pythonpath else str(_SRC_DIR)
    os.environ["PYTHONPATH"] = updated

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


pytestmark = [
    pytest.mark.skipif(not RUN_LOCAL, reason="Set RUN_SPARK_TESTS=1 to run Spark-local test"),
    pytest.mark.slow,
]


@contextmanager
def _stub_awsglue_modules() -> None:
    """Provide minimal awsglue shims required by the ETL script when running locally."""

    from pyspark.sql import SparkSession  # type: ignore

    awsglue_module = ModuleType("awsglue")

    context_module = ModuleType("awsglue.context")

    class GlueContext:  # type: ignore[too-few-public-methods]
        def __init__(self, spark_context):
            self.spark_context = spark_context
            self.spark_session = SparkSession(spark_context)

    context_module.GlueContext = GlueContext

    job_module = ModuleType("awsglue.job")

    class Job:  # type: ignore[too-few-public-methods]
        def __init__(self, glue_context):
            self.glue_context = glue_context
            self.initialized = False

        def init(self, _job_name, _args) -> None:
            self.initialized = True

        def commit(self) -> None:
            return None

    job_module.Job = Job

    utils_module = ModuleType("awsglue.utils")

    def getResolvedOptions(argv, options):
        results: dict[str, str] = {}
        for opt in options:
            flag = f"--{opt}"
            if flag not in argv:
                raise KeyError(f"{flag} not provided")
            idx = argv.index(flag)
            try:
                results[opt] = argv[idx + 1]
            except IndexError as exc:  # pragma: no cover - defensive guard
                raise KeyError(f"Missing value for {flag}") from exc
        return results

    utils_module.getResolvedOptions = getResolvedOptions  # type: ignore[attr-defined]

    modules = {
        "awsglue": awsglue_module,
        "awsglue.context": context_module,
        "awsglue.job": job_module,
        "awsglue.utils": utils_module,
    }

    previous = {name: sys.modules.get(name) for name in modules}
    try:
        for name, module in modules.items():
            sys.modules[name] = module
        yield
    finally:
        for name in modules:
            if previous[name] is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous[name]


@pytest.mark.integration
@pytest.mark.glue
@mock_aws
def test_indicators_etl_with_file_scheme() -> None:
    """
    Given: 로컬 file:// 기반 curared 입력과 Spark 실행 환경
    When: market_indicators_etl 스크립트를 실행
    Then: 기술 지표 Parquet과 최신 스키마 파일이 생성됨
    """
    s3 = boto3.client("s3", region_name="us-east-1")
    artifacts_bucket = "test-artifacts-bucket"
    s3.create_bucket(Bucket=artifacts_bucket)

    ds = "2025-09-07"
    interval = "1d"
    data_source = "yahoo_finance"
    with TemporaryDirectory() as tmp:
        in_dir = os.path.join(
            tmp,
            "market",
            "prices",
            f"interval={interval}",
            f"data_source={data_source}",
            "year=2025",
            "month=09",
            "day=07",
            "layer=adjusted",
        )
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
        )
        in_file = os.path.join(in_dir, "data.parquet")
        df.to_parquet(in_file, engine="pyarrow", index=False)

        argv = [
            "script.py",
            "--JOB_NAME",
            "test-indicators-job",
            "--environment",
            "test",
            "--prices_curated_bucket",
            tmp,
            "--prices_layer",
            "adjusted",
            "--output_bucket",
            tmp,
            "--output_layer",
            "technical_indicator",
            "--interval",
            interval,
            "--data_source",
            data_source,
            "--domain",
            "market",
            "--table_name",
            "prices",
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

        import sys as _sys

        prev_argv = list(_sys.argv)
        with _stub_awsglue_modules():
            try:
                _sys.argv = argv
                runpy.run_path("src/glue/jobs/market_indicators_etl.py", run_name="__main__")
            finally:
                _sys.argv = prev_argv

        out_dir = os.path.join(
            tmp,
            "market",
            "prices",
            f"interval={interval}",
            f"data_source={data_source}",
            "year=2025",
            "month=09",
            "day=07",
            "layer=technical_indicator",
        )
        files: list[str] = []
        for root, _dirs, filenames in os.walk(out_dir):
            for name in filenames:
                files.append(os.path.join(root, name))
        assert any(name.endswith(".parquet") for name in files), "Indicators parquet should be written"

        obj = s3.get_object(Bucket=artifacts_bucket, Key="market/prices/indicators/_schema/latest.json")
        assert obj["ResponseMetadata"]["HTTPStatusCode"] == 200
