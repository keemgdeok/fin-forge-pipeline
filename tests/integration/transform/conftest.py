"""
Integration Test Configuration for Transform Domain

Transform 도메인 통합 테스트를 위한 공통 fixtures와 설정을 제공합니다.
"""

import pytest
import os
from typing import Dict, Any
from moto import mock_aws


@pytest.fixture(scope="session")
def aws_credentials():
    """Session-scoped AWS credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def daily_batch_env() -> Dict[str, str]:
    """Essential environment variables for ETL integration tests"""
    return {
        "ENVIRONMENT": "test",
        "TARGET_DATE": "2025-09-09",
        "RAW_BUCKET": "finge-raw-test",
        "CURATED_BUCKET": "finge-curated-test",
    }


@pytest.fixture
def integration_test_buckets() -> Dict[str, str]:
    """Standard bucket names for integration tests"""
    return {
        "raw": "test-integration-raw-bucket",
        "curated": "test-integration-curated-bucket",
        "artifacts": "test-integration-artifacts-bucket",
    }


@pytest.fixture
def sample_transform_config() -> Dict[str, Any]:
    """Sample transform configuration for testing"""
    return {
        "domain": "market",
        "table_name": "prices",
        "glue_job_name": "test-market-prices-transform",
        "state_machine_name": "test-transform-workflow",
        "crawler_name": "test-curated-data-crawler",
        "database_name": "test_transform_db",
    }


@pytest.fixture(scope="function")
def mock_aws_services():
    """Mock all AWS services used in transform tests (moto >= 5)."""
    with mock_aws():
        yield


@pytest.fixture
def test_market_data():
    """Standard market data for testing"""
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
    ]


@pytest.fixture
def test_daily_prices_orders():
    """Standard daily prices order data for testing"""
    return [
        {
            "order_id": "ORD001",
            "symbol": "AAPL",
            "quantity": 2,
            "unit_price": 99.99,
            "trade_date": "2025-09-07",
            "status": "completed",
        },
        {
            "order_id": "ORD002",
            "symbol": "MSFT",
            "quantity": 1,
            "unit_price": 149.99,
            "trade_date": "2025-09-07",
            "status": "pending",
        },
    ]


def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line("markers", "integration: mark test as integration test")
    config.addinivalue_line("markers", "robustness: mark test as robustness/stress test")
    config.addinivalue_line("markers", "performance: mark test as performance test")
