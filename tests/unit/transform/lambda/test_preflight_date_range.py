"""Unit tests for Preflight Lambda date_range expansion.

Verifies that Preflight expands date_range into per-date items, performs
idempotency checks per date, and includes correct glue_args per item.
"""

from typing import Dict, Any, List
from importlib import import_module

import boto3
import pytest
from moto import mock_aws


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


def _set_pipeline_env(monkeypatch: pytest.MonkeyPatch, *, raw: str, curated: str, artifacts: str) -> None:
    monkeypatch.setenv("ENVIRONMENT", "test")
    monkeypatch.setenv("RAW_BUCKET", raw)
    monkeypatch.setenv("CURATED_BUCKET", curated)
    monkeypatch.setenv("ARTIFACTS_BUCKET", artifacts)
    monkeypatch.setenv("EXPECTED_MIN_RECORDS", "10")
    monkeypatch.setenv("MAX_CRITICAL_ERROR_RATE", "5.0")


@mock_aws
def test_date_range_expands_items_with_idempotency_and_glue_args(monkeypatch: pytest.MonkeyPatch) -> None:
    lambda_handler = import_module("src.lambda.functions.preflight.handler").lambda_handler

    s3 = boto3.client("s3", region_name="us-east-1")
    raw_b = "test-raw-bucket"
    curated_b = "test-curated-bucket"
    artifacts_b = "test-artifacts-bucket"
    s3.create_bucket(Bucket=curated_b)

    _set_pipeline_env(monkeypatch, raw=raw_b, curated=curated_b, artifacts=artifacts_b)

    # Pre-create a curated object for 2025-09-02 to trigger idempotent skip for that date
    s3.put_object(
        Bucket=curated_b,
        Key="market/prices/ds=2025-09-02/_SUCCESS",
        Body=b"",
        ContentType="text/plain",
    )

    event: Dict[str, Any] = {
        "domain": "market",
        "table_name": "prices",
        "file_type": "json",
        "date_range": {"start": "2025-09-01", "end": "2025-09-03"},
        "catalog_update": "on_schema_change",
    }

    out = lambda_handler(event, {})
    assert "dates" in out
    items: List[Dict[str, Any]] = out["dates"]
    assert len(items) == 3

    # Check each date's proceed/idempotency and glue args
    by_ds = {str(i.get("ds")): i for i in items}

    # 2025-09-01 and 2025-09-03 should proceed
    for d in ["2025-09-01", "2025-09-03"]:
        assert by_ds[d]["proceed"] is True
        gargs = by_ds[d]["glue_args"]
        assert gargs["--ds"] == d
        assert gargs["--raw_bucket"] == raw_b
        assert gargs["--curated_bucket"] == curated_b
        assert gargs["--schema_fingerprint_s3_uri"].startswith(
            f"s3://{artifacts_b}/market/prices/_schema/latest.json".rsplit("/latest.json", 1)[0]
        )
        assert by_ds[d]["catalog_update"] == "on_schema_change"

    # 2025-09-02 should be idempotent skip
    d2 = by_ds["2025-09-02"]
    assert d2["proceed"] is False
    assert d2["error"]["code"] == "IDEMPOTENT_SKIP"
    assert d2["catalog_update"] == "on_schema_change"


@mock_aws
def test_date_range_respects_max_backfill_days(monkeypatch: pytest.MonkeyPatch) -> None:
    lambda_handler = import_module("src.lambda.functions.preflight.handler").lambda_handler

    s3 = boto3.client("s3", region_name="us-east-1")
    curated_b = "test-curated-bucket"
    s3.create_bucket(Bucket=curated_b)

    _set_pipeline_env(monkeypatch, raw="r", curated=curated_b, artifacts="a")
    monkeypatch.setenv("MAX_BACKFILL_DAYS", "2")

    event = {
        "domain": "market",
        "table_name": "prices",
        "file_type": "json",
        "date_range": {"start": "2025-09-01", "end": "2025-09-05"},
    }

    out = lambda_handler(event, {})
    assert "dates" in out
    items = out["dates"]
    # Clamped to 2 days
    assert len(items) == 2
    assert items[0]["ds"] == "2025-09-01"
    assert items[1]["ds"] == "2025-09-02"
