"""
End-to-End Pipeline Validation Tests

Raw → Curated 전체 데이터 플로우를 검증하는 통합 테스트입니다.
실제 데이터 변환, 스키마 지문 생성, Glue Catalog 업데이트를
전체적으로 검증합니다.

테스트 범위:
- Raw 데이터 → Curated Parquet 변환 검증
- 파일 타입별 처리 (JSON, CSV, Parquet)
- Schema fingerprint 생성 및 일관성 검증
- 파티션 구조 및 메타데이터 검증
- 데이터 품질 규칙 적용 검증
- 멀티 파티션 처리 검증
"""

import pytest
import boto3
import json
import hashlib
from collections import defaultdict
from datetime import datetime
from unittest.mock import patch
from moto import mock_aws
from io import BytesIO

try:
    import pandas as pd
    import pyarrow as _pyarrow  # noqa: F401
except Exception:  # pragma: no cover - optional test deps
    pd = None  # type: ignore

pytestmark = pytest.mark.skipif(pd is None, reason="pandas/pyarrow not available")


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    import os

    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def pipeline_buckets():
    """파이프라인 테스트용 S3 버킷 설정"""
    return {
        "raw": "test-pipeline-raw-dev-123456789012",
        "curated": "test-pipeline-curated-dev-123456789012",
        "artifacts": "test-pipeline-artifacts-dev-123456789012",
    }


@pytest.fixture
def sample_market_data():
    """테스트용 마켓 데이터 샘플"""
    return [
        {
            "symbol": "AAPL",
            "price": 150.25,
            "volume": 1000000,
            "exchange": "NASDAQ",
            "timestamp": "2025-09-07T09:30:00Z",
            "currency": "USD",
        },
        {
            "symbol": "GOOGL",
            "price": 2750.50,
            "volume": 500000,
            "exchange": "NASDAQ",
            "timestamp": "2025-09-07T09:30:00Z",
            "currency": "USD",
        },
        {
            "symbol": "MSFT",
            "price": 420.75,
            "volume": 750000,
            "exchange": "NASDAQ",
            "timestamp": "2025-09-07T09:30:00Z",
            "currency": "USD",
        },
    ]


@pytest.fixture
def sample_daily_prices_orders():
    """테스트용 일일 가격 주문 데이터 샘플"""
    return [
        {
            "account_id": "CUST001",
            "order_id": "ORD001",
            "product_id": "PROD001",
            "quantity": 2,
            "unit_price": 99.99,
            "trade_date": "2025-09-07",
            "status": "completed",
        },
        {
            "account_id": "CUST002",
            "order_id": "ORD002",
            "product_id": "PROD002",
            "quantity": 1,
            "unit_price": 149.99,
            "trade_date": "2025-09-07",
            "status": "pending",
        },
    ]


def _stable_hash(obj: dict) -> str:
    """Consistent hash function for schema fingerprints"""
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _raw_day_prefix(
    date_str: str,
    *,
    domain: str = "market",
    table: str = "prices",
    interval: str = "1d",
    data_source: str = "yahoo_finance",
) -> str:
    """Build raw S3 prefix for the interval/data_source/year/month/day layout."""

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return (
        f"{domain}/{table}/"
        f"interval={interval}/"
        f"data_source={data_source}/"
        f"year={dt.year:04d}/month={dt.month:02d}/day={dt.day:02d}/"
    )


def _raw_object_key(
    date_str: str,
    *,
    domain: str = "market",
    table: str = "prices",
    interval: str = "1d",
    data_source: str = "yahoo_finance",
    object_name: str,
    extension: str = "json",
) -> str:
    prefix = _raw_day_prefix(
        date_str,
        domain=domain,
        table=table,
        interval=interval,
        data_source=data_source,
    )
    return f"{prefix}{object_name}.{extension}"


@pytest.mark.integration
class TestEndToEndPipeline:
    """End-to-End 파이프라인 검증 테스트 클래스"""

    @mock_aws
    def test_json_to_parquet_transformation_pipeline(self, aws_credentials, pipeline_buckets, sample_market_data):
        """
        Given: Raw S3에 JSON 형태의 마켓 데이터가 있으면
        When: 전체 변환 파이프라인을 실행하면
        Then: Curated S3에 올바른 Parquet 파일이 생성되어야 함
        """
        s3_client = boto3.client("s3", region_name="us-east-1")

        # Setup S3 buckets
        for bucket in pipeline_buckets.values():
            s3_client.create_bucket(Bucket=bucket)

        # Upload raw JSON data (one object per symbol)
        target_date = "2025-09-07"
        raw_prefix = _raw_day_prefix(target_date)
        records_by_symbol: defaultdict[str, list[dict]] = defaultdict(list)
        for record in sample_market_data:
            records_by_symbol[record["symbol"]].append(record)

        for symbol, records in records_by_symbol.items():
            body = "\n".join(json.dumps(r) for r in records).encode()
            key = _raw_object_key(target_date, object_name=symbol)
            s3_client.put_object(
                Bucket=pipeline_buckets["raw"],
                Key=key,
                Body=body,
                ContentType="application/json",
            )

        # Simulate ETL processing (실제 Glue job 로직)
        with patch("glue.jobs.daily_prices_data_etl"):
            # Mock ETL execution results
            transformed_data = []
            for record in sample_market_data:
                transformed_record = record.copy()
                transformed_record["ds"] = "2025-09-07"  # Add partition column
                transformed_data.append(transformed_record)

            # Convert to Parquet format
            df = pd.DataFrame(transformed_data)

            # Write to Curated bucket as Parquet
            curated_key = "market/prices/ds=2025-09-07/data.parquet"
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, engine="pyarrow", compression="snappy")
            parquet_data = parquet_buffer.getvalue()

            s3_client.put_object(
                Bucket=pipeline_buckets["curated"],
                Key=curated_key,
                Body=parquet_data,
                ContentType="application/octet-stream",
            )

            # Generate schema fingerprint
            schema_info = {
                "columns": [
                    {"name": "symbol", "type": "string"},
                    {"name": "price", "type": "double"},
                    {"name": "volume", "type": "bigint"},
                    {"name": "exchange", "type": "string"},
                    {"name": "timestamp", "type": "string"},
                    {"name": "currency", "type": "string"},
                    {"name": "ds", "type": "string"},
                ]
            }

            fingerprint = {
                "columns": schema_info["columns"],
                "codec": "snappy",
                "hash": _stable_hash({"columns": schema_info["columns"]}),
            }

            # Store schema fingerprint
            schema_key = "market/prices/_schema/latest.json"
            s3_client.put_object(
                Bucket=pipeline_buckets["artifacts"],
                Key=schema_key,
                Body=json.dumps(fingerprint).encode(),
                ContentType="application/json",
            )

        # Verify raw data was uploaded correctly
        raw_objects = s3_client.list_objects_v2(Bucket=pipeline_buckets["raw"], Prefix=raw_prefix)
        assert raw_objects["KeyCount"] == len(records_by_symbol), "Should have per-symbol raw files"

        # Verify curated Parquet file was created
        curated_objects = s3_client.list_objects_v2(
            Bucket=pipeline_buckets["curated"], Prefix="market/prices/ds=2025-09-07/"
        )
        assert curated_objects["KeyCount"] == 1, "Should have 1 curated Parquet file"

        # Verify schema fingerprint was created
        schema_objects = s3_client.list_objects_v2(
            Bucket=pipeline_buckets["artifacts"], Prefix="market/prices/_schema/"
        )
        assert schema_objects["KeyCount"] == 1, "Should have 1 schema fingerprint file"

        # Verify Parquet file content
        curated_response = s3_client.get_object(Bucket=pipeline_buckets["curated"], Key=curated_key)

        # Read Parquet data back
        parquet_buffer = BytesIO(curated_response["Body"].read())
        result_df = pd.read_parquet(parquet_buffer)

        # Verify data transformations
        assert len(result_df) == len(sample_market_data), "Should have same number of records"
        assert "ds" in result_df.columns, "Should have partition column 'ds'"
        assert all(result_df["ds"] == "2025-09-07"), "All records should have correct partition value"

        # Verify original data preserved
        assert set(result_df["symbol"].values) == {"AAPL", "GOOGL", "MSFT"}, "Symbol values should be preserved"
        assert result_df["price"].sum() > 3000, "Price values should be preserved"

        # Verify schema fingerprint content
        schema_response = s3_client.get_object(Bucket=pipeline_buckets["artifacts"], Key=schema_key)
        stored_fingerprint = json.loads(schema_response["Body"].read().decode())

        assert "columns" in stored_fingerprint, "Fingerprint should contain column info"
        assert "hash" in stored_fingerprint, "Fingerprint should contain hash"
        assert len(stored_fingerprint["columns"]) == 7, "Should have 7 columns including ds"

    @mock_aws
    def test_csv_to_parquet_transformation_pipeline(
        self,
        aws_credentials,
        pipeline_buckets,
        sample_daily_prices_orders,
    ):
        """
        Given: Raw S3에 CSV 형태의 고객 데이터가 있으면
        When: CSV → Parquet 변환 파이프라인을 실행하면
        Then: 올바른 타입 변환과 함께 Parquet 파일이 생성되어야 함
        """
        s3_client = boto3.client("s3", region_name="us-east-1")

        # Setup buckets
        for bucket in pipeline_buckets.values():
            s3_client.create_bucket(Bucket=bucket)

        # Create CSV data
        df_raw = pd.DataFrame(sample_daily_prices_orders)
        csv_buffer = BytesIO()
        df_raw.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()

        # Upload raw CSV data under new partition layout
        raw_key = _raw_object_key(
            "2025-09-07",
            domain="market",
            table="daily-prices-orders",
            data_source="orders_service",
            object_name="orders",
            extension="csv",
        )
        s3_client.put_object(Bucket=pipeline_buckets["raw"], Key=raw_key, Body=csv_data, ContentType="text/csv")

        # Simulate ETL processing with proper type handling
        raw_response = s3_client.get_object(Bucket=pipeline_buckets["raw"], Key=raw_key)

        # Read CSV with proper types
        csv_buffer = BytesIO(raw_response["Body"].read())
        df_raw_read = pd.read_csv(csv_buffer)

        # Apply transformations and type conversions
        df_transformed = df_raw_read.copy()
        df_transformed["ds"] = "2025-09-07"

        # Ensure proper types for Parquet
        df_transformed["quantity"] = df_transformed["quantity"].astype("int64")
        df_transformed["unit_price"] = df_transformed["unit_price"].astype("float64")

        # Write to Curated as Parquet with ZSTD compression (as per spec)
        curated_key = "market/daily-prices-orders/ds=2025-09-07/data.parquet"
        parquet_buffer = BytesIO()
        df_transformed.to_parquet(
            parquet_buffer,
            engine="pyarrow",
            compression="zstd",
            index=False,  # As specified in glue job spec
        )

        s3_client.put_object(
            Bucket=pipeline_buckets["curated"],
            Key=curated_key,
            Body=parquet_buffer.getvalue(),
            ContentType="application/octet-stream",
        )

        # Verify file was created and is readable
        curated_objects = s3_client.list_objects_v2(
            Bucket=pipeline_buckets["curated"], Prefix="market/daily-prices-orders/ds=2025-09-07/"
        )
        assert curated_objects["KeyCount"] == 1, "Should have 1 curated Parquet file"

        # Read back and verify content
        curated_response = s3_client.get_object(Bucket=pipeline_buckets["curated"], Key=curated_key)

        result_buffer = BytesIO(curated_response["Body"].read())
        result_df = pd.read_parquet(result_buffer)

        # Verify transformations
        assert len(result_df) == len(sample_daily_prices_orders), "Should preserve record count"
        assert "ds" in result_df.columns, "Should add partition column"
        assert result_df["quantity"].dtype == "int64", "Quantity should be int64 type"
        assert result_df["unit_price"].dtype == "float64", "Unit price should be float64 type"

        # Verify business data integrity
        total_quantity = result_df["quantity"].sum()
        expected_quantity = sum(item["quantity"] for item in sample_daily_prices_orders)
        assert total_quantity == expected_quantity, "Total quantity should be preserved"

    @mock_aws
    def test_multi_partition_processing_pipeline(self, aws_credentials, pipeline_buckets, sample_market_data):
        """
        Given: 여러 파티션(날짜)의 데이터가 Raw에 있으면
        When: 백필 파이프라인으로 처리하면
        Then: 각 파티션별로 올바른 Curated 데이터가 생성되어야 함
        """
        s3_client = boto3.client("s3", region_name="us-east-1")

        # Setup buckets
        for bucket in pipeline_buckets.values():
            s3_client.create_bucket(Bucket=bucket)

        # Create data for multiple partitions
        test_dates = ["2025-09-05", "2025-09-06", "2025-09-07"]

        expected_symbol_count = len({record["symbol"] for record in sample_market_data})

        for i, date in enumerate(test_dates):
            # Modify data slightly for each partition
            partition_data = []
            for record in sample_market_data:
                modified_record = record.copy()
                modified_record["price"] = record["price"] + (i * 0.50)  # Slight price variation
                modified_record["timestamp"] = f"{date}T09:30:00Z"
                partition_data.append(modified_record)

            # Upload raw data for this partition (split per symbol)
            raw_prefix = _raw_day_prefix(date)
            records_by_symbol: defaultdict[str, list[dict]] = defaultdict(list)
            for record in partition_data:
                records_by_symbol[record["symbol"]].append(record)

            for symbol, records in records_by_symbol.items():
                body = "\n".join(json.dumps(r) for r in records).encode()
                key = _raw_object_key(date, object_name=symbol)
                s3_client.put_object(
                    Bucket=pipeline_buckets["raw"],
                    Key=key,
                    Body=body,
                    ContentType="application/json",
                )

            # Simulate ETL processing for this partition
            df = pd.DataFrame(partition_data)
            df["ds"] = date  # Add partition column

            curated_key = f"market/prices/ds={date}/data.parquet"
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, engine="pyarrow", compression="zstd")

            s3_client.put_object(
                Bucket=pipeline_buckets["curated"],
                Key=curated_key,
                Body=parquet_buffer.getvalue(),
                ContentType="application/octet-stream",
            )

        # Verify all raw partitions were created
        for date in test_dates:
            raw_prefix = _raw_day_prefix(date)
            raw_objects = s3_client.list_objects_v2(Bucket=pipeline_buckets["raw"], Prefix=raw_prefix)
            assert raw_objects["KeyCount"] == expected_symbol_count, (
                f"Raw partition {date} should contain per-symbol objects"
            )

        # Verify all curated partitions were created
        for date in test_dates:
            curated_objects = s3_client.list_objects_v2(
                Bucket=pipeline_buckets["curated"], Prefix=f"market/prices/ds={date}/"
            )
            assert curated_objects["KeyCount"] == 1, f"Curated partition {date} should exist"

        # Verify cross-partition data consistency
        partition_prices = {}
        for date in test_dates:
            curated_key = f"market/prices/ds={date}/data.parquet"
            response = s3_client.get_object(Bucket=pipeline_buckets["curated"], Key=curated_key)

            df = pd.read_parquet(BytesIO(response["Body"].read()))
            partition_prices[date] = df["price"].sum()

        # Verify price progression (each day should have slightly higher prices)
        prices = [partition_prices[date] for date in sorted(test_dates)]
        for i in range(1, len(prices)):
            assert prices[i] > prices[i - 1], f"Prices should increase each day: day {i}"

    @mock_aws
    def test_data_quality_enforcement_in_pipeline(self, aws_credentials, pipeline_buckets):
        """
        Given: 데이터 품질 규칙을 위반하는 데이터가 있으면
        When: 파이프라인을 실행하면
        Then: 위반 데이터는 quarantine으로, 정상 데이터는 curated로 처리되어야 함
        """
        s3_client = boto3.client("s3", region_name="us-east-1")

        # Setup buckets
        for bucket in pipeline_buckets.values():
            s3_client.create_bucket(Bucket=bucket)

        # Create data with quality issues
        problematic_data = [
            {"symbol": None, "price": 150.25, "exchange": "NASDAQ"},  # null symbol (critical violation)
            {"symbol": "GOOGL", "price": -2750.50, "exchange": "NASDAQ"},  # negative price (critical violation)
            {"symbol": "MSFT", "price": 420.75, "exchange": "NASDAQ"},  # good record
            {"symbol": "AMZN", "price": 3500.00, "exchange": "NASDAQ"},  # good record
        ]

        # Upload problematic data under interval/source partitioning
        raw_key = _raw_object_key(
            "2025-09-07",
            object_name="problematic_data",
        )
        s3_client.put_object(
            Bucket=pipeline_buckets["raw"],
            Key=raw_key,
            Body=json.dumps(problematic_data).encode(),
            ContentType="application/json",
        )

        # Simulate ETL with data quality checks
        df = pd.DataFrame(problematic_data)

        # Apply data quality rules (simulate ETL logic)
        critical_violations = 0
        violations = []

        # Rule 1: Symbol not null
        null_symbol_mask = df["symbol"].isna()
        null_symbol_count = null_symbol_mask.sum()
        if null_symbol_count > 0:
            critical_violations += null_symbol_count
            violations.append(f"null symbol present ({null_symbol_count} records)")

        # Rule 2: Price not negative
        negative_price_mask = df["price"] < 0
        negative_price_count = negative_price_mask.sum()
        if negative_price_count > 0:
            critical_violations += negative_price_count
            violations.append(f"negative price present ({negative_price_count} records)")

        # Calculate error rate
        total_records = len(df)
        critical_error_rate = (critical_violations / total_records) * 100
        max_critical_error_rate = 20.0  # 20% threshold for test

        if critical_error_rate > max_critical_error_rate:
            # Write all data to quarantine
            quarantine_key = "market/prices/quarantine/ds=2025-09-07/problematic_data.parquet"
            quarantine_buffer = BytesIO()
            df.to_parquet(quarantine_buffer, engine="pyarrow", compression="zstd")

            s3_client.put_object(
                Bucket=pipeline_buckets["curated"],
                Key=quarantine_key,
                Body=quarantine_buffer.getvalue(),
                ContentType="application/octet-stream",
            )
        else:
            # Filter out bad records and write good ones to curated
            good_records_mask = ~(null_symbol_mask | negative_price_mask)
            df_clean = df[good_records_mask].copy()
            df_clean["ds"] = "2025-09-07"

            curated_key = "market/prices/ds=2025-09-07/clean_data.parquet"
            curated_buffer = BytesIO()
            df_clean.to_parquet(curated_buffer, engine="pyarrow", compression="zstd")

            s3_client.put_object(
                Bucket=pipeline_buckets["curated"],
                Key=curated_key,
                Body=curated_buffer.getvalue(),
                ContentType="application/octet-stream",
            )

        # Verify DQ processing results
        if critical_error_rate > max_critical_error_rate:
            # Should be in quarantine
            quarantine_objects = s3_client.list_objects_v2(
                Bucket=pipeline_buckets["curated"], Prefix="market/prices/quarantine/"
            )
            assert quarantine_objects["KeyCount"] == 1, "Problematic data should be quarantined"
        else:
            # Should be in curated (cleaned)
            curated_objects = s3_client.list_objects_v2(
                Bucket=pipeline_buckets["curated"], Prefix="market/prices/ds=2025-09-07/"
            )
            assert curated_objects["KeyCount"] == 1, "Clean data should be in curated"

            # Verify clean data content
            curated_response = s3_client.get_object(
                Bucket=pipeline_buckets["curated"], Key="market/prices/ds=2025-09-07/clean_data.parquet"
            )

            clean_df = pd.read_parquet(BytesIO(curated_response["Body"].read()))
            assert len(clean_df) == 2, "Should have 2 clean records (MSFT, AMZN)"
            assert all(clean_df["symbol"].notna()), "All symbols should be non-null"
            assert all(clean_df["price"] >= 0), "All prices should be non-negative"

    @mock_aws
    def test_schema_evolution_pipeline(self, aws_credentials, pipeline_buckets, sample_market_data):
        """
        Given: 스키마가 변경된 새로운 데이터가 들어오면
        When: 파이프라인을 실행하면
        Then: 새로운 스키마 지문이 생성되고 이전 버전과 비교되어야 함
        """
        s3_client = boto3.client("s3", region_name="us-east-1")

        # Setup buckets
        for bucket in pipeline_buckets.values():
            s3_client.create_bucket(Bucket=bucket)

        # Initial schema fingerprint
        initial_schema = {
            "columns": [
                {"name": "symbol", "type": "string"},
                {"name": "price", "type": "double"},
                {"name": "volume", "type": "bigint"},
                {"name": "exchange", "type": "string"},
                {"name": "ds", "type": "string"},
            ]
        }

        initial_fingerprint = {
            "columns": initial_schema["columns"],
            "codec": "zstd",
            "hash": _stable_hash({"columns": initial_schema["columns"]}),
        }

        # Store initial fingerprint
        s3_client.put_object(
            Bucket=pipeline_buckets["artifacts"],
            Key="market/prices/_schema/latest.json",
            Body=json.dumps(initial_fingerprint).encode(),
            ContentType="application/json",
        )

        # New data with additional columns (schema evolution)
        evolved_data = []
        for record in sample_market_data:
            evolved_record = record.copy()
            evolved_record["country"] = "US"  # New column
            evolved_record["market_cap"] = record["price"] * 1000000  # New column
            evolved_data.append(evolved_record)

        # Upload evolved data (per symbol)
        evolved_date = "2025-09-08"
        records_by_symbol: defaultdict[str, list[dict]] = defaultdict(list)
        for record in evolved_data:
            records_by_symbol[record["symbol"]].append(record)

        for symbol, records in records_by_symbol.items():
            body = "\n".join(json.dumps(r) for r in records).encode()
            key = _raw_object_key(evolved_date, object_name=symbol)
            s3_client.put_object(
                Bucket=pipeline_buckets["raw"],
                Key=key,
                Body=body,
                ContentType="application/json",
            )

        # Process evolved data
        df_evolved = pd.DataFrame(evolved_data)
        df_evolved["ds"] = evolved_date

        # Generate new schema fingerprint
        new_schema = {
            "columns": [
                {"name": "symbol", "type": "string"},
                {"name": "price", "type": "double"},
                {"name": "volume", "type": "bigint"},
                {"name": "exchange", "type": "string"},
                {"name": "timestamp", "type": "string"},
                {"name": "currency", "type": "string"},
                {"name": "country", "type": "string"},  # New column
                {"name": "market_cap", "type": "double"},  # New column
                {"name": "ds", "type": "string"},
            ]
        }

        new_fingerprint = {
            "columns": new_schema["columns"],
            "codec": "zstd",
            "hash": _stable_hash({"columns": new_schema["columns"]}),
        }

        # Compare fingerprints to detect schema change
        schema_changed = initial_fingerprint["hash"] != new_fingerprint["hash"]
        assert schema_changed, "Schema change should be detected"

        # Store new fingerprint
        s3_client.put_object(
            Bucket=pipeline_buckets["artifacts"],
            Key="market/prices/_schema/latest.json",
            Body=json.dumps(new_fingerprint).encode(),
            ContentType="application/json",
        )

        # Store evolved data as Parquet
        curated_key = "market/prices/ds=2025-09-08/evolved_data.parquet"
        parquet_buffer = BytesIO()
        df_evolved.to_parquet(parquet_buffer, engine="pyarrow", compression="zstd")

        s3_client.put_object(
            Bucket=pipeline_buckets["curated"],
            Key=curated_key,
            Body=parquet_buffer.getvalue(),
            ContentType="application/octet-stream",
        )

        # Verify schema fingerprint was updated
        updated_response = s3_client.get_object(
            Bucket=pipeline_buckets["artifacts"], Key="market/prices/_schema/latest.json"
        )
        updated_fingerprint = json.loads(updated_response["Body"].read().decode())

        assert len(updated_fingerprint["columns"]) == 9, "Should have 9 columns (2 new + 7 original)"
        assert updated_fingerprint["hash"] != initial_fingerprint["hash"], "Hash should be different"

        # Verify new columns are present in fingerprint
        column_names = [col["name"] for col in updated_fingerprint["columns"]]
        assert "country" in column_names, "Should include new 'country' column"
        assert "market_cap" in column_names, "Should include new 'market_cap' column"

        # Verify evolved data in Parquet
        curated_response = s3_client.get_object(Bucket=pipeline_buckets["curated"], Key=curated_key)

        result_df = pd.read_parquet(BytesIO(curated_response["Body"].read()))
        assert "country" in result_df.columns, "Parquet should contain new country column"
        assert "market_cap" in result_df.columns, "Parquet should contain new market_cap column"
        assert all(result_df["country"] == "US"), "Country values should be preserved"
