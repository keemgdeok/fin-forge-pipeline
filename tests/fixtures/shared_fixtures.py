"""Shared test fixtures and utilities for transform pipeline testing.

This module provides reusable test fixtures, mock objects, and utility functions
that follow the transform specifications and support TDD methodology across
all transform pipeline unit tests.
"""

import json
import pytest
import os
from typing import Dict, Any, List, Optional, Union
from datetime import date, datetime, timedelta
from dataclasses import dataclass, field
from unittest.mock import Mock


@dataclass
class S3ObjectFixture:
    """Test representation of an S3 object."""

    bucket: str
    key: str
    content: Union[str, bytes, Dict[str, Any]]
    content_type: str = "application/json"
    metadata: Optional[Dict[str, str]] = None


@dataclass
class PartitionFixture:
    """Test representation of a data partition."""

    domain: str
    table_name: str
    ds: str
    interval: str = "1d"
    data_source: str = "yahoo_finance"
    symbols: List[str] = field(default_factory=lambda: ["AAPL"])
    file_type: str = "json"
    record_count: int = 100

    @property
    def raw_prefix(self) -> str:
        """Get raw S3 prefix for this partition."""
        partition_date = date.fromisoformat(self.ds)
        return (
            f"{self.domain}/{self.table_name}/"
            f"interval={self.interval}/"
            f"data_source={self.data_source}/"
            f"year={partition_date.year:04d}/"
            f"month={partition_date.month:02d}/"
            f"day={partition_date.day:02d}/"
        )

    @property
    def curated_prefix(self) -> str:
        """Get curated S3 prefix for this partition."""
        return f"{self.domain}/{self.table_name}/ds={self.ds}/"

    def raw_key(self, symbol: Optional[str] = None) -> str:
        """Build the raw object key for a given symbol (defaults to first symbol)."""

        target_symbol = symbol or (self.symbols[0] if self.symbols else "symbol")
        return f"{self.raw_prefix}{target_symbol}.{self.file_type}"


class MockAWSEnvironment:
    """Mock AWS environment with configurable resources."""

    def __init__(self, environment: str = "test"):
        self.environment = environment
        self.raw_bucket = f"data-pipeline-raw-{environment}-123456789012"
        self.curated_bucket = f"data-pipeline-curated-{environment}-123456789012"
        self.artifacts_bucket = f"data-pipeline-artifacts-{environment}-123456789012"
        self.s3_objects: Dict[str, S3ObjectFixture] = {}

    def add_s3_object(self, obj: S3ObjectFixture) -> None:
        """Add an S3 object to the mock environment."""
        key = f"{obj.bucket}/{obj.key}"
        self.s3_objects[key] = obj

    def get_s3_object(self, bucket: str, key: str) -> Optional[S3ObjectFixture]:
        """Get an S3 object from the mock environment."""
        lookup_key = f"{bucket}/{key}"
        return self.s3_objects.get(lookup_key)

    def list_s3_objects(self, bucket: str, prefix: str) -> List[S3ObjectFixture]:
        """List S3 objects matching a prefix."""
        results = []
        for obj_key, obj in self.s3_objects.items():
            if obj_key.startswith(f"{bucket}/{prefix}"):
                results.append(obj)
        return results

    def has_partition(self, partition: PartitionFixture, bucket_type: str = "curated") -> bool:
        """Check if a partition exists in the specified bucket."""
        bucket = getattr(self, f"{bucket_type}_bucket")
        prefix = getattr(partition, f"{bucket_type}_prefix")
        return len(self.list_s3_objects(bucket, prefix)) > 0

    def create_raw_partition(
        self,
        partition: PartitionFixture,
        data: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """Create raw objects per symbol under the interval/source partition."""

        for symbol in partition.symbols:
            if data is None:
                symbol_records = generate_sample_market_data(
                    partition.record_count,
                    symbol=symbol,
                )
            else:
                symbol_records = []
                for record in data:
                    record_copy = dict(record)
                    record_copy.setdefault("symbol", symbol)
                    symbol_records.append(record_copy)

            if partition.file_type == "json":
                content: Union[str, bytes] = "\n".join(json.dumps(record) for record in symbol_records)
                content_type = "application/json"
            elif partition.file_type == "csv":
                if symbol_records:
                    headers = ",".join(symbol_records[0].keys())
                    rows = [
                        ",".join(str(record.get(key, "")) for key in symbol_records[0].keys())
                        for record in symbol_records
                    ]
                    content = headers + "\n" + "\n".join(rows)
                else:
                    content = ""
                content_type = "text/csv"
            else:  # parquet - simplified as JSON payload for tests
                content = json.dumps(symbol_records)
                content_type = "application/octet-stream"

            self.add_s3_object(
                S3ObjectFixture(
                    bucket=self.raw_bucket,
                    key=partition.raw_key(symbol),
                    content=content,
                    content_type=content_type,
                )
            )

    def create_curated_partition(self, partition: PartitionFixture) -> None:
        """Create a curated partition to simulate existing data."""
        s3_key = f"{partition.curated_prefix}part-00000.parquet"
        self.add_s3_object(
            S3ObjectFixture(
                bucket=self.curated_bucket,
                key=s3_key,
                content=b"mock_parquet_data",
                content_type="application/octet-stream",
            )
        )

    def create_schema_fingerprint(self, partition: PartitionFixture, schema: Optional[Dict[str, Any]] = None) -> None:
        """Create a schema fingerprint for a partition."""
        if schema is None:
            schema = generate_sample_schema()

        artifacts_key = f"{partition.domain}/{partition.table_name}/_schema/latest.json"
        self.add_s3_object(
            S3ObjectFixture(
                bucket=self.artifacts_bucket,
                key=artifacts_key,
                content=schema,
                content_type="application/json",
            )
        )

    def create_current_schema(self, partition: PartitionFixture, schema: Optional[Dict[str, Any]] = None) -> None:
        """Create a current schema file for a partition."""
        if schema is None:
            schema = generate_sample_schema()

        curated_key = f"{partition.domain}/{partition.table_name}/_schema/current.json"
        self.add_s3_object(
            S3ObjectFixture(
                bucket=self.curated_bucket,
                key=curated_key,
                content=schema,
                content_type="application/json",
            )
        )


class MockS3Client:
    """Mock S3 client that works with MockAWSEnvironment."""

    def __init__(self, aws_env: MockAWSEnvironment):
        self.aws_env = aws_env
        self.call_history: List[Dict[str, Any]] = []

    def list_objects_v2(self, Bucket: str, Prefix: str, MaxKeys: int = 1000) -> Dict[str, Any]:
        """Mock S3 list_objects_v2 operation."""
        self.call_history.append(
            {
                "operation": "list_objects_v2",
                "bucket": Bucket,
                "prefix": Prefix,
                "max_keys": MaxKeys,
            }
        )

        objects = self.aws_env.list_s3_objects(Bucket, Prefix)
        return {
            "KeyCount": len(objects),
            "Contents": [
                {
                    "Key": obj.key,
                    "Size": len(str(obj.content)),
                    "LastModified": datetime.utcnow(),
                }
                for obj in objects[:MaxKeys]
            ],
        }

    def get_object(self, Bucket: str, Key: str) -> Dict[str, Any]:
        """Mock S3 get_object operation."""
        self.call_history.append({"operation": "get_object", "bucket": Bucket, "key": Key})

        obj = self.aws_env.get_s3_object(Bucket, Key)
        if obj is None:
            from botocore.exceptions import ClientError

            raise ClientError(
                error_response={"Error": {"Code": "NoSuchKey"}},
                operation_name="GetObject",
            )

        if isinstance(obj.content, dict):
            body_content = json.dumps(obj.content).encode("utf-8")
        elif isinstance(obj.content, str):
            body_content = obj.content.encode("utf-8")
        else:
            body_content = obj.content

        return {
            "Body": MockS3Body(body_content),
            "ContentType": obj.content_type,
            "Metadata": obj.metadata or {},
        }

    def put_object(
        self,
        Bucket: str,
        Key: str,
        Body: Union[str, bytes],
        ContentType: str = "application/json",
        **kwargs,
    ) -> Dict[str, Any]:
        """Mock S3 put_object operation."""
        self.call_history.append(
            {
                "operation": "put_object",
                "bucket": Bucket,
                "key": Key,
                "content_type": ContentType,
                "body_length": len(Body) if Body else 0,
            }
        )

        content = Body
        if isinstance(Body, bytes):
            try:
                # Try to decode as JSON for easier testing
                content = json.loads(Body.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass  # Keep as bytes if not JSON

        self.aws_env.add_s3_object(
            S3ObjectFixture(
                bucket=Bucket,
                key=Key,
                content=content,
                content_type=ContentType,
            )
        )

        return {"ResponseMetadata": {"HTTPStatusCode": 200}}


class MockS3Body:
    """Mock S3 response body object."""

    def __init__(self, data: bytes):
        self.data = data

    def read(self) -> bytes:
        return self.data


def generate_sample_market_data(count: int = 100, symbol: str = "AAPL") -> List[Dict[str, Any]]:
    """Generate sample market data for testing."""
    base_time = datetime(2025, 9, 7, 10, 0, 0)
    base_price = 150.0

    data = []
    for i in range(count):
        timestamp = base_time + timedelta(minutes=i)
        price = base_price + (i * 0.1) + ((i % 10) - 5) * 0.5  # Some variation

        record = {
            "symbol": (f"{symbol}{i % 3}" if count > 3 else symbol),  # Vary symbols for larger datasets
            "price": round(price, 2),
            "exchange": "NASDAQ" if i % 2 == 0 else "NYSE",
            "timestamp": timestamp.isoformat() + "Z",
            "volume": 1000 + (i * 10),
        }
        data.append(record)

    return data


def generate_sample_schema(include_hash: bool = True) -> Dict[str, Any]:
    """Generate a sample schema fingerprint."""
    schema = {
        "columns": [
            {"name": "symbol", "type": "string"},
            {"name": "price", "type": "double"},
            {"name": "exchange", "type": "string"},
            {"name": "timestamp", "type": "timestamp"},
            {"name": "volume", "type": "long"},
        ],
        "codec": "zstd",
    }

    if include_hash:
        # Generate a consistent hash for testing
        import hashlib

        schema_str = json.dumps(schema["columns"], sort_keys=True)
        schema["hash"] = hashlib.sha256(schema_str.encode()).hexdigest()

    return schema


def create_test_date_range(start_date: str, days: int) -> List[str]:
    """Create a list of date strings for testing date ranges."""
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    return [(start + timedelta(days=i)).isoformat() for i in range(days)]


@pytest.fixture
def mock_aws_environment() -> MockAWSEnvironment:
    """Provide a clean mock AWS environment for each test."""
    return MockAWSEnvironment("test")


@pytest.fixture
def sample_partition() -> PartitionFixture:
    """Provide a standard test partition."""
    return PartitionFixture(
        domain="market",
        table_name="prices",
        ds="2025-09-07",
        file_type="json",
        record_count=50,
    )


@pytest.fixture
def market_data_partition(
    mock_aws_environment: MockAWSEnvironment, sample_partition: PartitionFixture
) -> PartitionFixture:
    """Provide a partition with sample market data."""
    mock_aws_environment.create_raw_partition(sample_partition)
    return sample_partition


@pytest.fixture
def transform_env_vars(
    mock_aws_environment: MockAWSEnvironment,
) -> Dict[str, str]:
    """Provide standard environment variables for transform testing."""
    env_vars = {
        "ENVIRONMENT": mock_aws_environment.environment,
        "RAW_BUCKET": mock_aws_environment.raw_bucket,
        "CURATED_BUCKET": mock_aws_environment.curated_bucket,
        "ARTIFACTS_BUCKET": mock_aws_environment.artifacts_bucket,
        "MAX_BACKFILL_DAYS": "31",
        "CATALOG_UPDATE_DEFAULT": "on_schema_change",
    }

    # Set in actual environment for tests that read os.environ
    for key, value in env_vars.items():
        os.environ[key] = value

    return env_vars


def assert_error_contract(
    response: Dict[str, Any],
    expected_code: str,
    expected_fields: Optional[List[str]] = None,
) -> None:
    """Assert that a response follows the error contract specification."""
    assert "error" in response, "Response should contain error field"
    assert "code" in response["error"], "Error should contain code field"
    assert response["error"]["code"] == expected_code, (
        f"Expected error code {expected_code}, got {response['error']['code']}"
    )
    assert "message" in response["error"], "Error should contain message field"

    if expected_fields:
        for field in expected_fields:
            assert field in response, f"Response should contain field {field}"


def assert_success_contract(response: Dict[str, Any], expected_fields: List[str]) -> None:
    """Assert that a response follows the success contract specification."""
    for expected_field in expected_fields:
        assert expected_field in response, f"Response should contain field {expected_field}"

    # Should not contain error field in success response
    assert "error" not in response, "Success response should not contain error field"


def assert_glue_args_valid(
    glue_args: Dict[str, str],
    partition: PartitionFixture,
    aws_env: MockAWSEnvironment,
) -> None:
    """Assert that Glue arguments follow the specification."""
    required_args = [
        "--ds",
        "--raw_bucket",
        "--raw_prefix",
        "--curated_bucket",
        "--curated_prefix",
        "--environment",
        "--schema_fingerprint_s3_uri",
        "--codec",
        "--target_file_mb",
        "--file_type",
        "--output_partitions",
    ]

    for arg in required_args:
        assert arg in glue_args, f"Glue args should contain {arg}"

    # Validate specific values
    assert glue_args["--ds"] == partition.ds
    assert glue_args["--raw_bucket"] == aws_env.raw_bucket
    assert glue_args["--curated_bucket"] == aws_env.curated_bucket
    expected_raw_prefix = (
        f"{partition.domain}/{partition.table_name}/interval={partition.interval}/data_source={partition.data_source}/"
    )
    assert glue_args["--raw_prefix"] == expected_raw_prefix
    assert glue_args["--curated_prefix"] == f"{partition.domain}/{partition.table_name}/"
    assert glue_args["--codec"] == "zstd"
    assert glue_args["--target_file_mb"] == "256"
    assert glue_args["--file_type"] == partition.file_type
    assert glue_args.get("--output_partitions") is not None

    # Validate S3 URI format
    fingerprint_uri = glue_args["--schema_fingerprint_s3_uri"]
    assert fingerprint_uri.startswith("s3://"), "Schema fingerprint URI should be S3 URI"
    expected_suffix = f"{partition.domain}/{partition.table_name}/_schema/latest.json"
    assert fingerprint_uri.endswith(expected_suffix), f"Schema fingerprint URI should end with {expected_suffix}"


def create_mock_lambda_context(function_name: str = "test-function", request_id: str = "test-123") -> Mock:
    """Create a mock Lambda context object."""
    context = Mock()
    context.function_name = function_name
    context.aws_request_id = request_id
    context.log_group_name = f"/aws/lambda/{function_name}"
    context.memory_limit_in_mb = 512
    context.get_remaining_time_in_millis.return_value = 30000
    return context


def create_step_functions_event(partition: PartitionFixture, event_type: str = "direct") -> Dict[str, Any]:
    """Create a Step Functions input event for testing."""
    if event_type == "direct":
        return {
            "domain": partition.domain,
            "table_name": partition.table_name,
            "ds": partition.ds,
            "file_type": partition.file_type,
        }
    elif event_type == "s3_trigger":
        return {
            "source_bucket": "raw-bucket-test",
            "source_key": partition.raw_key(),
            "domain": partition.domain,
            "table_name": partition.table_name,
            "file_type": partition.file_type,
        }
    elif event_type == "backfill":
        start_date = date.fromisoformat(partition.ds)
        end_date = start_date + timedelta(days=2)
        return {
            "domain": partition.domain,
            "table_name": partition.table_name,
            "date_range": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
            },
            "file_type": partition.file_type,
        }
    else:
        raise ValueError(f"Unknown event type: {event_type}")


class MockBoto3Session:
    """Mock boto3 session for testing."""

    def __init__(self, aws_env: MockAWSEnvironment):
        self.aws_env = aws_env
        self.clients: Dict[str, Any] = {}

    def client(self, service_name: str, **kwargs) -> Any:
        """Create a mock client for the specified service."""
        if service_name == "s3":
            return MockS3Client(self.aws_env)
        else:
            # Return a generic mock for other services
            return Mock()


@pytest.fixture(autouse=True)
def clean_environment():
    """Clean up environment variables after each test."""
    original_env = dict(os.environ)
    yield
    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)


def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers",
        "slow: marks tests as slow (deselect with '-m \"not slow\"')",
    )
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "unit: marks tests as unit tests")
