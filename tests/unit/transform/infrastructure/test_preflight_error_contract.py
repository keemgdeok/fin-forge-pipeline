import os
import runpy
from typing import Any, List


def _load_module():
    return runpy.run_path("src/lambda/functions/preflight/handler.py")


class _S3Stub:
    def __init__(self, keycount: int = 0):
        self._keycount = keycount
        self.calls: List[Any] = []

    def list_objects_v2(self, Bucket: str, Prefix: str, MaxKeys: int = 1):
        self.calls.append((Bucket, Prefix))
        return {"KeyCount": self._keycount}


class _Boto:
    def __init__(self, s3_stub: _S3Stub):
        self._s3 = s3_stub

    def client(self, name: str):
        assert name == "s3"
        return self._s3


def test_missing_required_fields_returns_pre_validation_failed(
    monkeypatch,
) -> None:
    """
    Given: 필수 필드가 비어 있는 이벤트와 필수 버킷 env 설정
    When: preflight lambda_handler 검증 실행
    Then: proceed False, PRE_VALIDATION_FAILED 코드
    """
    mod = _load_module()
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["RAW_BUCKET"] = "raw-bucket-dev"
    os.environ["CURATED_BUCKET"] = "curated-bucket-dev"
    os.environ["ARTIFACTS_BUCKET"] = "artifacts-bucket-dev"

    s3 = _S3Stub(0)
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    resp = mod["lambda_handler"]({}, None)
    assert resp["proceed"] is False
    assert resp["error"]["code"] == "PRE_VALIDATION_FAILED"


def test_missing_date_segments_in_key_returns_pre_validation_failed(
    monkeypatch,
) -> None:
    """
    Given: 날짜 파티션 경로가 없는 source_key
    When: preflight lambda_handler 검증 실행
    Then: PRE_VALIDATION_FAILED 코드
    """
    mod = _load_module()
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["RAW_BUCKET"] = "raw-bucket-dev"
    os.environ["CURATED_BUCKET"] = "curated-bucket-dev"
    os.environ["ARTIFACTS_BUCKET"] = "artifacts-bucket-dev"

    s3 = _S3Stub(0)
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    event = {
        "source_bucket": "raw-bucket-dev",
        "source_key": "market/prices/interval=1d/data_source=yahoo_finance/AAPL.json",
        "domain": "market",
        "table_name": "prices",
        "file_type": "json",
    }
    resp = mod["lambda_handler"](event, None)
    assert resp["proceed"] is False
    assert resp["error"]["code"] == "PRE_VALIDATION_FAILED"


def test_missing_bucket_env_returns_pre_validation_failed(monkeypatch) -> None:
    """
    Given: RAW/CURATED/ARTIFACTS env 미설정
    When: 유효한 이벤트로 preflight 호출
    Then: proceed False, PRE_VALIDATION_FAILED 코드
    """
    mod = _load_module()
    # Intentionally do not set bucket envs
    os.environ.pop("RAW_BUCKET", None)
    os.environ.pop("CURATED_BUCKET", None)
    os.environ.pop("ARTIFACTS_BUCKET", None)
    os.environ["ENVIRONMENT"] = "dev"

    s3 = _S3Stub(0)
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    event = {
        "source_bucket": "raw-bucket-dev",
        "source_key": ("market/prices/interval=1d/data_source=yahoo_finance/year=2025/month=09/day=07/AAPL.json"),
        "domain": "market",
        "table_name": "prices",
        "file_type": "json",
    }
    resp = mod["lambda_handler"](event, None)
    assert resp["proceed"] is False
    assert resp["error"]["code"] == "PRE_VALIDATION_FAILED"


def test_idempotent_skip_returns_expected_code_and_ds(monkeypatch) -> None:
    """
    Given: 대상 파티션이 이미 curated에 존재
    When: preflight lambda_handler 호출
    Then: proceed False, ds 유지, IDEMPOTENT_SKIP 코드
    """
    mod = _load_module()
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["RAW_BUCKET"] = "raw-bucket-dev"
    os.environ["CURATED_BUCKET"] = "curated-bucket-dev"
    os.environ["ARTIFACTS_BUCKET"] = "artifacts-bucket-dev"

    # KeyCount=1 -> curated partition exists
    s3 = _S3Stub(1)
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    event = {
        "source_bucket": "raw-bucket-dev",
        "source_key": ("market/prices/interval=1d/data_source=yahoo_finance/year=2025/month=09/day=07/AAPL.json"),
        "domain": "market",
        "table_name": "prices",
        "file_type": "json",
    }
    resp = mod["lambda_handler"](event, None)
    assert resp["proceed"] is False
    assert resp["ds"] == "2025-09-07"
    assert resp["error"]["code"] == "IDEMPOTENT_SKIP"


def test_valid_builds_glue_args_with_schema_path(monkeypatch) -> None:
    """
    Given: 정상 파티션 키와 필수 버킷 env
    When: preflight lambda_handler 검증 통과
    Then: proceed True, schema_fingerprint_s3_uri 최신 경로
    """
    mod = _load_module()
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["RAW_BUCKET"] = "raw-bucket-dev"
    os.environ["CURATED_BUCKET"] = "curated-bucket-dev"
    os.environ["ARTIFACTS_BUCKET"] = "artifacts-bucket-dev"

    s3 = _S3Stub(0)
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    event = {
        "source_bucket": "raw-bucket-dev",
        "source_key": ("market/prices/interval=1d/data_source=yahoo_finance/year=2025/month=09/day=07/AAPL.json"),
        "domain": "market",
        "table_name": "prices",
        "file_type": "json",
    }
    resp = mod["lambda_handler"](event, None)
    assert resp["proceed"] is True
    assert resp["ds"] == "2025-09-07"
    schema_arg = resp["glue_args"]["--schema_fingerprint_s3_uri"]
    assert schema_arg.endswith("market/prices/_schema/latest.json")


def test_valid_includes_threshold_args(monkeypatch) -> None:
    """
    Given: 커스텀 임계값 env 값
    When: preflight lambda_handler Glue 인자 생성
    Then: expected_min_records, max_critical_error_rate 지정 값 유지
    """
    mod = _load_module()
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["RAW_BUCKET"] = "raw-bucket-dev"
    os.environ["CURATED_BUCKET"] = "curated-bucket-dev"
    os.environ["ARTIFACTS_BUCKET"] = "artifacts-bucket-dev"
    # Inject non-default thresholds
    os.environ["EXPECTED_MIN_RECORDS"] = "123"
    os.environ["MAX_CRITICAL_ERROR_RATE"] = "7.5"

    s3 = _S3Stub(0)
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    event = {
        "source_bucket": "raw-bucket-dev",
        "source_key": ("market/prices/interval=1d/data_source=yahoo_finance/year=2025/month=09/day=07/AAPL.json"),
        "domain": "market",
        "table_name": "prices",
        "file_type": "json",
    }
    resp = mod["lambda_handler"](event, None)
    assert resp["proceed"] is True
    args = resp["glue_args"]
    assert args["--expected_min_records"] == "123"
    assert args["--max_critical_error_rate"] == "7.5"
