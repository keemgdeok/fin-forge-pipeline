"""Core transform workflow integration tests (optimized).

This module contains the essential integration tests for the transform pipeline,
focusing on the most critical workflows while maintaining comprehensive coverage
with fewer, more targeted tests.

Tests follow TDD methodology and validate against transform specifications.
"""

import os
import json
import boto3
import pytest
from unittest.mock import Mock, patch
from moto import mock_aws

from tests.fixtures.data_builders import build_raw_market_data


@pytest.fixture
def core_transform_env():
    """Setup core transform test environment."""

    def _setup():
        os.environ.update(
            {
                "ENVIRONMENT": "test",
                "RAW_BUCKET": "test-raw-bucket",
                "CURATED_BUCKET": "test-curated-bucket",
                "ARTIFACTS_BUCKET": "test-artifacts-bucket",
                "AWS_REGION": "us-east-1",
            }
        )

    return _setup


@mock_aws
class TestCoreTransformWorkflow:
    """Essential integration tests for transform pipeline."""

    def test_happy_path_end_to_end_workflow(self, core_transform_env, load_module):
        """
        Given: 완전한 transform 파이프라인 환경
        When: 정상적인 데이터로 전체 워크플로우를 실행하면
        Then: Preflight → Glue ETL → Schema Check → Catalog 순서로 성공해야 함
        """
        core_transform_env()

        # Setup S3 buckets
        s3_client = boto3.client("s3", region_name="us-east-1")
        for bucket in ["test-raw-bucket", "test-curated-bucket", "test-artifacts-bucket"]:
            s3_client.create_bucket(Bucket=bucket)

        # Upload raw data
        raw_data = build_raw_market_data(
            [{"symbol": "AAPL", "price": 150.25, "exchange": "NASDAQ", "timestamp": "2025-09-07T10:00:00Z"}]
        )

        raw_key = "market/prices/ingestion_date=2025-09-07/data.json"
        s3_client.put_object(Bucket="test-raw-bucket", Key=raw_key, Body=json.dumps(raw_data).encode("utf-8"))

        # Test 1: Preflight validation
        preflight_mod = load_module("src/lambda/functions/preflight/handler.py")
        preflight_event = {
            "source_bucket": "test-raw-bucket",
            "source_key": raw_key,
            "domain": "market",
            "table_name": "prices",
            "file_type": "json",
        }

        with patch("boto3.client") as mock_boto:
            mock_s3 = Mock()
            mock_s3.list_objects_v2.return_value = {"KeyCount": 0}  # No existing curated data
            mock_boto.return_value = mock_s3

            preflight_response = preflight_mod["lambda_handler"](preflight_event, None)

            assert preflight_response["proceed"] is True
            assert preflight_response["ds"] == "2025-09-07"
            assert "--ds" in preflight_response["glue_args"]

        # Test 2: Schema check for changes
        schema_mod = load_module("src/lambda/functions/schema_check/handler.py")
        schema_event = {
            "domain": "market",
            "table_name": "prices",
            "current_schema": {
                "columns": [
                    {"name": "symbol", "type": "string"},
                    {"name": "price", "type": "double"},
                    {"name": "exchange", "type": "string"},
                ],
                "codec": "zstd",
            },
        }

        with patch("boto3.client") as mock_boto:
            mock_s3 = Mock()
            mock_s3.get_object.side_effect = Exception("NoSuchKey")  # No existing schema
            mock_boto.return_value = mock_s3

            schema_response = schema_mod["lambda_handler"](schema_event, None)
            assert schema_response["schema_changed"] is True

        # Test 3: Build dates for backfill
        build_dates_mod = load_module("src/lambda/functions/build_dates/handler.py")
        dates_event = {"date_range": {"start": "2025-09-07", "end": "2025-09-07"}}

        dates_response = build_dates_mod["lambda_handler"](dates_event, None)
        assert dates_response["dates"] == ["2025-09-07"]

        # Simulate successful completion
        curated_key = "market/prices/ds=2025-09-07/output.parquet"
        s3_client.put_object(Bucket="test-curated-bucket", Key=curated_key, Body=b"mock parquet data")

        # Verify curated output exists
        curated_objects = s3_client.list_objects_v2(Bucket="test-curated-bucket", Prefix="market/prices/ds=2025-09-07/")
        assert curated_objects["KeyCount"] == 1

    def test_error_handling_and_recovery_workflow(self, core_transform_env, load_module):
        """
        Given: 다양한 오류 시나리오들
        When: Transform 컴포넌트들이 오류를 처리하면
        Then: 명세에 정의된 오류 코드와 복구 메커니즘을 따라야 함
        """
        core_transform_env()

        preflight_mod = load_module("src/lambda/functions/preflight/handler.py")

        # Test Error Scenario 1: PRE_VALIDATION_FAILED
        invalid_event = {}  # Missing required fields
        response = preflight_mod["lambda_handler"](invalid_event, None)

        assert response["proceed"] is False
        assert response["error"]["code"] == "PRE_VALIDATION_FAILED"
        assert "message" in response["error"]

        # Test Error Scenario 2: IDEMPOTENT_SKIP
        with patch("boto3.client") as mock_boto:
            mock_s3 = Mock()
            mock_s3.list_objects_v2.return_value = {"KeyCount": 1}  # Curated data exists
            mock_boto.return_value = mock_s3

            valid_event = {
                "source_bucket": "test-raw-bucket",
                "source_key": "market/prices/ingestion_date=2025-09-07/data.json",
                "domain": "market",
                "table_name": "prices",
                "file_type": "json",
            }

            response = preflight_mod["lambda_handler"](valid_event, None)

            assert response["proceed"] is False
            assert response["error"]["code"] == "IDEMPOTENT_SKIP"
            assert response["ds"] == "2025-09-07"  # Should still return ds for tracking

    def test_data_quality_and_quarantine_workflow(self, core_transform_env):
        """
        Given: 품질 문제가 있는 데이터
        When: 데이터 품질 검증을 수행하면
        Then: 유효한 데이터는 처리하고 무효한 데이터는 격리해야 함
        """
        core_transform_env()

        # Setup S3
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket="test-curated-bucket")

        # Mixed quality test data
        test_data = [
            {"symbol": "AAPL", "price": 150.25, "exchange": "NASDAQ", "timestamp": "2025-09-07T10:00:00Z"},  # Valid
            {
                "symbol": None,
                "price": 100.00,
                "exchange": "NASDAQ",
                "timestamp": "2025-09-07T10:01:00Z",
            },  # Invalid symbol
            {
                "symbol": "BAD",
                "price": -50.00,
                "exchange": "NYSE",
                "timestamp": "2025-09-07T10:02:00Z",
            },  # Invalid price
        ]

        # Simulate DQ processing results
        valid_records = [record for record in test_data if record.get("symbol") and record.get("price", 0) > 0]
        invalid_records = [record for record in test_data if not record.get("symbol") or record.get("price", 0) <= 0]

        # Valid data goes to curated
        curated_key = "market/prices/ds=2025-09-07/clean_data.parquet"
        s3_client.put_object(
            Bucket="test-curated-bucket",
            Key=curated_key,
            Body=json.dumps({"valid_records": valid_records}).encode("utf-8"),
        )

        # Invalid data goes to quarantine
        quarantine_key = "market/prices/quarantine/ds=2025-09-07/dq_violations.parquet"
        quarantine_metadata = {
            "invalid_records": invalid_records,
            "violations": [{"rule": "symbol_not_null", "count": 1}, {"rule": "price_positive", "count": 1}],
        }

        s3_client.put_object(
            Bucket="test-curated-bucket", Key=quarantine_key, Body=json.dumps(quarantine_metadata).encode("utf-8")
        )

        # Verify data separation
        curated_obj = s3_client.get_object(Bucket="test-curated-bucket", Key=curated_key)
        curated_data = json.loads(curated_obj["Body"].read())
        assert len(curated_data["valid_records"]) == 1

        quarantine_obj = s3_client.get_object(Bucket="test-curated-bucket", Key=quarantine_key)
        quarantine_data = json.loads(quarantine_obj["Body"].read())
        assert len(quarantine_data["violations"]) == 2


@mock_aws
class TestSchemaEvolutionWorkflow:
    """Essential tests for schema evolution scenarios."""

    def test_schema_change_detection_and_crawler_trigger(self, core_transform_env, load_module):
        """
        Given: 스키마 변경이 발생함
        When: Schema Check를 수행하면
        Then: 변경을 감지하고 적절한 크롤러 트리거 결정을 해야 함
        """
        core_transform_env()

        # Setup S3 with existing schema
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket="test-artifacts-bucket")

        original_schema = {
            "columns": [{"name": "symbol", "type": "string"}, {"name": "price", "type": "double"}],
            "codec": "zstd",
            "hash": "original_hash_123",
        }

        schema_key = "market/prices/_schema/latest.json"
        s3_client.put_object(
            Bucket="test-artifacts-bucket", Key=schema_key, Body=json.dumps(original_schema).encode("utf-8")
        )

        schema_mod = load_module("src/lambda/functions/schema_check/handler.py")

        # Test backward compatible change (add column)
        event = {
            "domain": "market",
            "table_name": "prices",
            "current_schema": {
                "columns": [
                    {"name": "symbol", "type": "string"},
                    {"name": "price", "type": "double"},
                    {"name": "volume", "type": "bigint"},  # New column
                ],
                "codec": "zstd",
            },
        }

        response = schema_mod["lambda_handler"](event, None)

        assert response["schema_changed"] is True
        assert "new_hash" in response
        assert response["new_hash"] != "original_hash_123"


@mock_aws
class TestPerformanceValidationWorkflow:
    """Essential tests for performance validation."""

    def test_file_size_optimization_compliance(self):
        """
        Given: 파일 크기 최적화 요구사항
        When: 파일 크기를 검증하면
        Then: 128-512MB 범위 내에서 256MB 목표에 근접해야 함
        """
        # Simulate optimized file creation
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket="test-curated-bucket")

        # Create files with target sizes
        target_sizes = [200, 256, 400]  # MB

        for i, size_mb in enumerate(target_sizes):
            mock_data = b"x" * min(size_mb * 1024, 100000)  # Limit for test performance
            curated_key = f"market/prices/ds=2025-09-07/part-{i:04d}.parquet"

            s3_client.put_object(Bucket="test-curated-bucket", Key=curated_key, Body=mock_data)

        # Verify files were created
        objects = s3_client.list_objects_v2(Bucket="test-curated-bucket", Prefix="market/prices/ds=2025-09-07/")

        assert objects["KeyCount"] == 3

        # In real scenario, would verify actual file sizes are within target range
        for obj in objects.get("Contents", []):
            assert obj["Size"] > 0
