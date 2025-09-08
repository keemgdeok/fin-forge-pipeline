import os
import runpy
import pytest

# Test imports follow TDD methodology
from typing import Optional


def _load_module():
    return runpy.run_path("src/lambda/functions/preflight/handler.py")


class _S3Stub:
    def __init__(self, keycount: int = 0, should_raise: Optional[Exception] = None):
        self._keycount = keycount
        self._should_raise = should_raise
        self.calls = []

    def list_objects_v2(self, Bucket: str, Prefix: str, MaxKeys: int = 1):
        self.calls.append({"bucket": Bucket, "prefix": Prefix, "max_keys": MaxKeys})
        if self._should_raise:
            raise self._should_raise
        return {"KeyCount": self._keycount}


class _ClientError(Exception):
    """Mock botocore ClientError"""

    def __init__(self, error_code: str):
        self.response = {"Error": {"Code": error_code}}


class _Boto:
    def __init__(self, s3_stub: _S3Stub):
        self._s3 = s3_stub

    def client(self, name: str):
        assert name == "s3"
        return self._s3


def _setup_env(monkeypatch) -> None:
    """Setup common environment variables for tests"""
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["RAW_BUCKET"] = "raw-bucket-dev"
    os.environ["CURATED_BUCKET"] = "curated-bucket-dev"
    os.environ["ARTIFACTS_BUCKET"] = "artifacts-bucket-dev"


def test_preflight_supports_direct_ds_mode(monkeypatch) -> None:
    """
    Given: ds가 직접 입력되고 curated 파티션이 존재하지 않음
    When: Preflight를 실행하면
    Then: proceed=True, ds가 에코되고 Glue 인자가 포함되어야 함
    """
    mod = _load_module()
    _setup_env(monkeypatch)

    s3 = _S3Stub(keycount=0)
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    event = {
        "domain": "market",
        "table_name": "prices",
        "ds": "2025-09-07",
        "file_type": "json",
    }
    resp = mod["lambda_handler"](event, None)
    assert resp["proceed"] is True
    assert resp["ds"] == "2025-09-07"
    assert resp["glue_args"]["--ds"] == "2025-09-07"
    assert resp["glue_args"]["--schema_fingerprint_s3_uri"].endswith("market/prices/_schema/latest.json")

    # Verify idempotency check was performed
    assert len(s3.calls) == 1
    assert s3.calls[0]["prefix"] == "market/prices/ds=2025-09-07/"


def test_preflight_supports_s3_trigger_mode(monkeypatch) -> None:
    """
    Given: S3 트리거 모드로 source_bucket/source_key가 제공됨
    When: Preflight를 실행하면
    Then: key에서 ds를 추출하고 proceed=True를 반환해야 함
    """
    mod = _load_module()
    _setup_env(monkeypatch)

    s3 = _S3Stub(keycount=0)
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    event = {
        "source_bucket": "raw-bucket-dev",
        "source_key": "market/prices/ingestion_date=2025-09-07/file.json",
        "domain": "market",
        "table_name": "prices",
        "file_type": "json",
    }
    resp = mod["lambda_handler"](event, None)
    assert resp["proceed"] is True
    assert resp["ds"] == "2025-09-07"
    assert resp["glue_args"]["--file_type"] == "json"


def test_preflight_idempotent_skip_when_curated_exists(monkeypatch) -> None:
    """
    Given: curated 파티션이 이미 존재함
    When: Preflight를 실행하면
    Then: proceed=False, IDEMPOTENT_SKIP 에러를 반환해야 함
    """
    mod = _load_module()
    _setup_env(monkeypatch)

    # KeyCount > 0 indicates partition already exists
    s3 = _S3Stub(keycount=1)
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    event = {
        "domain": "market",
        "table_name": "prices",
        "ds": "2025-09-07",
        "file_type": "json",
    }
    resp = mod["lambda_handler"](event, None)
    assert resp["proceed"] is False
    assert resp["reason"] == "Already processed"
    assert resp["error"]["code"] == "IDEMPOTENT_SKIP"
    assert resp["ds"] == "2025-09-07"


def test_preflight_pre_validation_failed_missing_domain(monkeypatch) -> None:
    """
    Given: domain 필드가 누락됨
    When: Preflight를 실행하면
    Then: PRE_VALIDATION_FAILED 에러를 반환해야 함
    """
    mod = _load_module()
    _setup_env(monkeypatch)

    event = {
        "table_name": "prices",
        "ds": "2025-09-07",
    }
    resp = mod["lambda_handler"](event, None)
    assert resp["proceed"] is False
    assert resp["error"]["code"] == "PRE_VALIDATION_FAILED"
    assert "Missing domain/table_name" in resp["error"]["message"]


def test_preflight_pre_validation_failed_missing_table_name(
    monkeypatch,
) -> None:
    """
    Given: table_name 필드가 누락됨
    When: Preflight를 실행하면
    Then: PRE_VALIDATION_FAILED 에러를 반환해야 함
    """
    mod = _load_module()
    _setup_env(monkeypatch)

    event = {
        "domain": "market",
        "ds": "2025-09-07",
    }
    resp = mod["lambda_handler"](event, None)
    assert resp["proceed"] is False
    assert resp["error"]["code"] == "PRE_VALIDATION_FAILED"
    assert "Missing domain/table_name" in resp["error"]["message"]


def test_preflight_pre_validation_failed_s3_mode_missing_bucket(
    monkeypatch,
) -> None:
    """
    Given: S3 트리거 모드에서 source_bucket이 누락됨
    When: Preflight를 실행하면
    Then: PRE_VALIDATION_FAILED 에러를 반환해야 함
    """
    mod = _load_module()
    _setup_env(monkeypatch)

    event = {
        "source_key": "market/prices/ingestion_date=2025-09-07/file.json",
        "domain": "market",
        "table_name": "prices",
    }
    resp = mod["lambda_handler"](event, None)
    assert resp["proceed"] is False
    assert resp["error"]["code"] == "PRE_VALIDATION_FAILED"
    assert "Missing source_bucket/source_key" in resp["error"]["message"]


def test_preflight_pre_validation_failed_invalid_ingestion_date_format(
    monkeypatch,
) -> None:
    """
    Given: S3 key에 잘못된 ingestion_date 형식이 포함됨
    When: Preflight를 실행하면
    Then: PRE_VALIDATION_FAILED 에러를 반환해야 함
    """
    mod = _load_module()
    _setup_env(monkeypatch)

    s3 = _S3Stub(keycount=0)
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    event = {
        "source_bucket": "raw-bucket-dev",
        "source_key": "market/prices/ingestion_date=2025/09/07/file.json",  # Wrong format
        "domain": "market",
        "table_name": "prices",
    }
    resp = mod["lambda_handler"](event, None)
    assert resp["proceed"] is False
    assert resp["error"]["code"] == "PRE_VALIDATION_FAILED"
    assert "ingestion_date=YYYY-MM-DD not found" in resp["error"]["message"]


def test_preflight_pre_validation_failed_missing_bucket_env() -> None:
    """
    Given: 필수 환경변수(CURATED_BUCKET 등)가 누락됨
    When: Preflight를 실행하면
    Then: PRE_VALIDATION_FAILED 에러를 반환해야 함
    """
    mod = _load_module()
    # Don't setup full environment
    os.environ["ENVIRONMENT"] = "dev"
    # Missing CURATED_BUCKET, RAW_BUCKET, ARTIFACTS_BUCKET

    event = {
        "domain": "market",
        "table_name": "prices",
        "ds": "2025-09-07",
    }
    resp = mod["lambda_handler"](event, None)
    assert resp["proceed"] is False
    assert resp["error"]["code"] == "PRE_VALIDATION_FAILED"
    assert "Missing CURATED_BUCKET/RAW_BUCKET/ARTIFACTS_BUCKET" in resp["error"]["message"]


def test_preflight_handles_s3_client_error_gracefully(monkeypatch) -> None:
    """
    Given: S3 호출 시 ClientError가 발생함
    When: Preflight를 실행하면
    Then: idempotency check가 실패하고 False를 가정해야 함 (proceed=True)
    """
    mod = _load_module()
    _setup_env(monkeypatch)

    # Mock ClientError during S3 list operation
    s3 = _S3Stub(should_raise=_ClientError("AccessDenied"))
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    event = {
        "domain": "market",
        "table_name": "prices",
        "ds": "2025-09-07",
        "file_type": "json",
    }
    resp = mod["lambda_handler"](event, None)
    # Should proceed=True when S3 check fails (assumes partition doesn't exist)
    assert resp["proceed"] is True
    assert resp["ds"] == "2025-09-07"


def test_preflight_glue_args_construction_comprehensive(monkeypatch) -> None:
    """
    Given: 모든 필수 입력이 제공됨
    When: Preflight를 실행하면
    Then: 완전한 Glue 인자 집합을 생성해야 함
    """
    mod = _load_module()
    _setup_env(monkeypatch)

    s3 = _S3Stub(keycount=0)
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    event = {
        "domain": "customer-data",
        "table_name": "orders",
        "ds": "2025-09-08",
        "file_type": "parquet",
    }
    resp = mod["lambda_handler"](event, None)

    expected_args = {
        "--environment": "dev",
        "--raw_bucket": "raw-bucket-dev",
        "--raw_prefix": "customer-data/orders/",
        "--curated_bucket": "curated-bucket-dev",
        "--curated_prefix": "customer-data/orders/",
        "--schema_fingerprint_s3_uri": "s3://artifacts-bucket-dev/customer-data/orders/_schema/latest.json",
        "--codec": "zstd",
        "--target_file_mb": "256",
        "--ds": "2025-09-08",
        "--file_type": "parquet",
    }

    assert resp["proceed"] is True
    assert resp["glue_args"] == expected_args


def test_preflight_extract_ds_from_key_edge_cases(monkeypatch) -> None:
    """
    Given: 다양한 S3 key 패턴
    When: ds 추출을 시도하면
    Then: 올바른 패턴에서만 ds를 추출해야 함
    """
    mod = _load_module()
    _setup_env(monkeypatch)

    s3 = _S3Stub(keycount=0)
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    # Valid patterns
    valid_keys = [
        "market/prices/ingestion_date=2025-09-07/file.json",
        "customer-data/orders/ingestion_date=2023-12-31/data.parquet",
        "analytics/metrics/ingestion_date=2025-01-01/batch_001.csv",
    ]

    for key in valid_keys:
        event = {
            "source_bucket": "raw-bucket-dev",
            "source_key": key,
            "domain": "test",
            "table_name": "test",
        }
        resp = mod["lambda_handler"](event, None)
        assert resp["proceed"] is True
        # Extract expected date from key
        expected_date = key.split("ingestion_date=")[1].split("/")[0]
        assert resp["ds"] == expected_date


def test_preflight_file_type_defaults_to_json(monkeypatch) -> None:
    """
    Given: file_type이 제공되지 않음
    When: Preflight를 실행하면
    Then: file_type이 'json'으로 기본 설정되어야 함
    """
    mod = _load_module()
    _setup_env(monkeypatch)

    s3 = _S3Stub(keycount=0)
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    event = {
        "domain": "market",
        "table_name": "prices",
        "ds": "2025-09-07",
        # file_type not provided
    }
    resp = mod["lambda_handler"](event, None)
    assert resp["proceed"] is True
    assert resp["glue_args"]["--file_type"] == "json"


@pytest.mark.parametrize(
    "ds_value,expected_valid",
    [
        ("2025-09-07", True),
        ("2023-12-31", True),
        ("2025-01-01", True),
        ("", False),
        (None, False),
        (
            "invalid-date",
            True,
        ),  # Lambda doesn't validate date format, just passes through
    ],
)
def test_preflight_ds_validation_parametrized(monkeypatch, ds_value, expected_valid) -> None:
    """
    Given: 다양한 ds 값들
    When: Preflight를 실행하면
    Then: 적절한 검증 결과를 반환해야 함
    """
    mod = _load_module()
    _setup_env(monkeypatch)

    s3 = _S3Stub(keycount=0)
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    event = {
        "domain": "market",
        "table_name": "prices",
        "file_type": "json",
    }

    if ds_value is not None:
        event["ds"] = ds_value

    resp = mod["lambda_handler"](event, None)

    if expected_valid and ds_value:
        assert resp["proceed"] is True
        assert resp["ds"] == ds_value
    else:
        # Should fall back to S3 mode validation and fail
        assert resp["proceed"] is False
        assert resp["error"]["code"] == "PRE_VALIDATION_FAILED"
