"""
Minimal Integration Test Configuration

Only essential fixtures for remaining ETL data quality integration tests.
"""

import pytest
from typing import Dict


@pytest.fixture
def daily_batch_env() -> Dict[str, str]:
    """Essential environment variables for ETL integration tests"""
    return {
        "ENVIRONMENT": "test",
        "TARGET_DATE": "2025-09-09",
        "RAW_BUCKET": "finge-raw-test",
        "CURATED_BUCKET": "finge-curated-test",
    }
