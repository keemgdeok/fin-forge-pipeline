"""Practical fixtures for daily batch transform pipeline testing.

This module provides simplified test fixtures optimized for daily batch
processing scenarios with 1GB or less data. Focuses on essential functionality
without over-engineering for extreme scale scenarios.

Replaces the comprehensive conftest.py with a practical approach.
"""

import os
import pytest
import runpy
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from unittest.mock import Mock

from tests.fixtures.deterministic_data_builders import (
    build_deterministic_transform_event,
    build_deterministic_market_data,
)


@pytest.fixture
def daily_batch_env():
    """Setup simplified environment for daily batch testing."""

    def _setup():
        os.environ.update(
            {
                "ENVIRONMENT": "test",
                "RAW_BUCKET": "test-raw-bucket",
                "CURATED_BUCKET": "test-curated-bucket",
                "ARTIFACTS_BUCKET": "test-artifacts-bucket",
                "AWS_REGION": "us-east-1",
                "TARGET_FILE_MB": "128",
                "GLUE_MAX_DPU": "2",
                "MAX_PROCESSING_TIME_MINUTES": "30",
            }
        )

    return _setup


@pytest.fixture
def load_module():
    """Load Python modules dynamically for testing."""

    def _load(module_path: str):
        """Load a module by path for testing."""
        return runpy.run_path(module_path)

    return _load


@pytest.fixture
def practical_s3_mock():
    """Create simplified S3 mock for daily batch scenarios."""

    class PracticalS3Mock:
        def __init__(self):
            self.objects = {}
            self.buckets = {"test-raw-bucket", "test-curated-bucket", "test-artifacts-bucket"}

        def put_object(self, Bucket: str, Key: str, Body: bytes, **kwargs):
            self.objects[f"{Bucket}/{Key}"] = {
                "Body": Body,
                "Size": len(Body) if isinstance(Body, bytes) else len(str(Body)),
            }
            return {"ResponseMetadata": {"HTTPStatusCode": 200}}

        def get_object(self, Bucket: str, Key: str):
            obj_key = f"{Bucket}/{Key}"
            if obj_key not in self.objects:
                raise Exception("NoSuchKey")

            obj = self.objects[obj_key]
            return {"Body": Mock(read=lambda: obj["Body"])}

        def list_objects_v2(self, Bucket: str, Prefix: str = "", MaxKeys: int = 1000):
            matching_keys = []
            for key in self.objects.keys():
                if key.startswith(f"{Bucket}/") and key.split("/", 1)[1].startswith(Prefix):
                    matching_keys.append(key.split("/", 1)[1])

            return {"KeyCount": len(matching_keys), "Contents": [{"Key": key} for key in matching_keys]}

        def head_object(self, Bucket: str, Key: str):
            obj_key = f"{Bucket}/{Key}"
            if obj_key not in self.objects:
                raise Exception("NoSuchKey")
            return {"ContentLength": self.objects[obj_key]["Size"]}

    return PracticalS3Mock


@pytest.fixture
def daily_batch_data():
    """Generate practical test data (reduced size for fast execution)."""

    def _generate(
        batch_size: int = 10000,  # Reduced for practical testing
        corruption_rate: float = 0.05,  # 5% data issues (realistic)
        include_extended_fields: bool = False,
    ) -> List[Dict[str, Any]]:

        return build_deterministic_market_data(
            count=batch_size, corruption_rate=corruption_rate, include_extended_fields=include_extended_fields
        )

    return _generate


@pytest.fixture
def transform_event_builder():
    """Build realistic transform events for daily batch processing."""

    def _build(
        domain: str = "market", table_name: str = "prices", ds: Optional[str] = None, **kwargs
    ) -> Dict[str, Any]:

        if ds is None:
            ds = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        return build_deterministic_transform_event(domain=domain, table_name=table_name, ds=ds, **kwargs)

    return _build


@pytest.fixture
def daily_batch_thresholds():
    """Simplified thresholds for daily batch processing."""
    return {
        "quality_score": 0.90,  # 90% clean data expected
        "max_processing_time": 30,  # seconds for typical batch
        "target_file_mb": 128,  # Target file size
    }


@pytest.fixture
def common_error_codes():
    """Common error codes for daily batch processing."""
    return ["PRE_VALIDATION_FAILED", "IDEMPOTENT_SKIP", "NO_RAW_DATA", "DQ_FAILED"]


@pytest.fixture
def validate_response():
    """Simple response validation for daily batch."""

    def _validate_success(response):
        assert isinstance(response, dict)
        assert response.get("ok") is not False

    def _validate_error(response, expected_code=None):
        assert "error" in response
        assert "code" in response["error"]
        if expected_code:
            assert response["error"]["code"] == expected_code

    return {"success": _validate_success, "error": _validate_error}


# Simplified module-level fixtures for common daily batch scenarios
@pytest.fixture(scope="session")
def sample_daily_batch():
    """Pre-generated sample test batch for reuse across tests."""
    return build_deterministic_market_data(count=1000, corruption_rate=0.05)  # Small for reuse


@pytest.fixture(scope="session")
def sample_schema_fingerprint():
    """Standard schema fingerprint for daily batch market data."""
    return {
        "columns": [
            {"name": "symbol", "type": "string"},
            {"name": "price", "type": "double"},
            {"name": "exchange", "type": "string"},
            {"name": "timestamp", "type": "timestamp"},
            {"name": "volume", "type": "bigint"},
        ],
        "codec": "zstd",
        "hash": "daily_batch_schema_v1",
        "created_at": "2025-09-07T10:00:00Z",
    }


@pytest.fixture(autouse=True)
def cleanup_environment():
    """Automatically cleanup environment after each test."""
    yield

    # Clean up 8 core environment variables only
    core_env_vars = [
        "ENVIRONMENT",
        "RAW_BUCKET",
        "CURATED_BUCKET",
        "ARTIFACTS_BUCKET",
        "AWS_REGION",
        "TARGET_FILE_MB",
        "GLUE_MAX_DPU",
        "MAX_PROCESSING_TIME_MINUTES",
    ]

    for var in core_env_vars:
        if var in os.environ:
            del os.environ[var]
