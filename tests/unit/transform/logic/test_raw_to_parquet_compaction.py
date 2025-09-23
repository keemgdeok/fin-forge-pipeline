"""Unit tests for the raw_to_parquet_compaction Glue script."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Dict
from unittest.mock import MagicMock, patch

import pytest


def _base_args(file_type: str = "json") -> Dict[str, str]:
    return {
        "JOB_NAME": "unit-test-compaction",
        "raw_bucket": "raw-bucket",
        "raw_prefix": "market/prices/interval=1d/data_source=yahoo/",
        "compacted_bucket": "curated-bucket",
        "compacted_prefix": "market/prices/compacted",
        "ds": "2024-01-15",
        "file_type": file_type,
        "codec": "zstd",
        "target_file_mb": "128",
        "interval": "1d",
        "data_source": "yahoo",
    }


def _mock_spark_session(record_count: int) -> SimpleNamespace:
    mock_df = MagicMock()
    mock_df.count.return_value = record_count
    mock_writer = MagicMock()
    mock_writer.mode.return_value = mock_writer
    mock_writer.format.return_value = mock_writer
    mock_writer.option.return_value = mock_writer
    mock_df.write = mock_writer

    mock_reader = MagicMock()
    mock_reader.json.return_value = mock_df
    mock_reader.csv.return_value = mock_df
    mock_reader.parquet.return_value = mock_df

    mock_conf = MagicMock()

    spark_session = SimpleNamespace(read=mock_reader, conf=mock_conf)
    return spark_session, mock_df, mock_writer, mock_conf


def _load_compaction_module() -> ModuleType:
    """Load the compaction module with awsglue/pyspark stubs injected."""

    stub_modules: Dict[str, ModuleType] = {}

    awsglue_pkg = ModuleType("awsglue")
    stub_modules["awsglue"] = awsglue_pkg

    awsglue_context = ModuleType("awsglue.context")
    awsglue_context.GlueContext = object  # replaced with MagicMock in tests
    stub_modules["awsglue.context"] = awsglue_context

    awsglue_utils = ModuleType("awsglue.utils")

    def _dummy_get_resolved_options(*args, **kwargs):  # pragma: no cover - replaced in tests
        raise NotImplementedError

    awsglue_utils.getResolvedOptions = _dummy_get_resolved_options
    stub_modules["awsglue.utils"] = awsglue_utils

    awsglue_job = ModuleType("awsglue.job")
    awsglue_job.Job = object
    stub_modules["awsglue.job"] = awsglue_job

    pyspark_pkg = ModuleType("pyspark")
    stub_modules["pyspark"] = pyspark_pkg

    pyspark_context = ModuleType("pyspark.context")
    pyspark_context.SparkContext = object
    stub_modules["pyspark.context"] = pyspark_context

    pyspark_sql = ModuleType("pyspark.sql")

    class _DataFrame:  # pragma: no cover - placeholder for type hints
        pass

    class _SparkSession:  # pragma: no cover - placeholder for type hints
        pass

    pyspark_sql.DataFrame = _DataFrame
    pyspark_sql.SparkSession = _SparkSession
    stub_modules["pyspark.sql"] = pyspark_sql

    module_name = "_raw_to_parquet_compaction_under_test"
    module_path = Path("src/glue/jobs/raw_to_parquet_compaction.py")

    # Ensure no stale module remains between tests
    sys.modules.pop(module_name, None)
    sys.modules.pop("src.glue.jobs.raw_to_parquet_compaction", None)

    with patch.dict(sys.modules, stub_modules, clear=False):
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)

    return module


def test_compaction_writes_parquet(monkeypatch):
    compaction = _load_compaction_module()
    args = _base_args()

    spark_session, mock_df, mock_writer, mock_conf = _mock_spark_session(record_count=10)
    mock_glue_context = SimpleNamespace(spark_session=spark_session)
    mock_job = MagicMock()
    mock_job.commit = MagicMock()

    mock_s3_client = MagicMock()
    mock_s3_client.list_objects_v2.return_value = {"KeyCount": 5}

    with (
        patch.object(compaction, "getResolvedOptions", return_value=args),
        patch.object(compaction, "SparkContext"),
        patch.object(compaction, "GlueContext", return_value=mock_glue_context),
        patch.object(compaction, "Job", return_value=mock_job),
        patch.object(compaction.boto3, "client", return_value=mock_s3_client),
    ):
        compaction.main()

    expected_raw_path = "s3://raw-bucket/market/prices/interval=1d/data_source=yahoo/year=2024/month=01/day=15/"
    spark_session.read.json.assert_called_once_with(expected_raw_path)

    mock_conf.set.assert_any_call("spark.sql.parquet.compression.codec", "zstd")
    mock_conf.set.assert_any_call("spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024))

    mock_writer.save.assert_called_once_with("s3://curated-bucket/market/prices/compacted/ds=2024-01-15")
    mock_job.commit.assert_called_once()


def test_compaction_skips_when_no_raw_objects(monkeypatch):
    compaction = _load_compaction_module()
    args = _base_args()

    spark_session, mock_df, mock_writer, _ = _mock_spark_session(record_count=10)
    mock_glue_context = SimpleNamespace(spark_session=spark_session)
    mock_job = MagicMock()

    mock_s3_client = MagicMock()
    mock_s3_client.list_objects_v2.return_value = {"KeyCount": 0}

    with (
        patch.object(compaction, "getResolvedOptions", return_value=args),
        patch.object(compaction, "SparkContext"),
        patch.object(compaction, "GlueContext", return_value=mock_glue_context),
        patch.object(compaction, "Job", return_value=mock_job),
        patch.object(compaction.boto3, "client", return_value=mock_s3_client),
    ):
        compaction.main()

    spark_session.read.json.assert_not_called()
    mock_writer.save.assert_not_called()
    mock_job.commit.assert_called_once()


def test_compaction_raises_for_unknown_file_type(monkeypatch):
    compaction = _load_compaction_module()
    args = _base_args(file_type="xml")

    spark_session, _, _, _ = _mock_spark_session(record_count=10)
    mock_glue_context = SimpleNamespace(spark_session=spark_session)
    mock_job = MagicMock()

    mock_s3_client = MagicMock()
    mock_s3_client.list_objects_v2.return_value = {"KeyCount": 1}

    with (
        patch.object(compaction, "getResolvedOptions", return_value=args),
        patch.object(compaction, "SparkContext"),
        patch.object(compaction, "GlueContext", return_value=mock_glue_context),
        patch.object(compaction, "Job", return_value=mock_job),
        patch.object(compaction.boto3, "client", return_value=mock_s3_client),
    ):
        with pytest.raises(ValueError, match="Unsupported file_type"):
            compaction.main()

    mock_job.commit.assert_not_called()
