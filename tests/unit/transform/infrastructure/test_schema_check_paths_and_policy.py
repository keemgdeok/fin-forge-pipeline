import os
import json
import runpy
import pytest

# Test imports follow TDD methodology
from typing import Optional


def _load_module():
    return runpy.run_path("src/lambda/functions/schema_check/handler.py")


class _S3Recorder:
    def __init__(
        self,
        latest: dict | None,
        current: dict | None = None,
        should_raise: Optional[Exception] = None,
    ):
        self.latest = latest
        self.current = current
        self.get_calls = []
        self.put_calls = []
        self._should_raise = should_raise

    # Simulate S3 get/put for curated/artifacts
    def get_object(self, Bucket: str, Key: str):
        self.get_calls.append((Bucket, Key))
        if self._should_raise:
            raise self._should_raise
        if Key.endswith("_schema/latest.json") and self.latest is not None:
            body = json.dumps(self.latest).encode("utf-8")
            return {"Body": _Body(body)}
        if Key.endswith("_schema/current.json") and self.current is not None:
            body = json.dumps(self.current).encode("utf-8")
            return {"Body": _Body(body)}
        # Simulate not found for others
        raise _NoSuchKey()

    def put_object(self, Bucket: str, Key: str, Body: bytes, ContentType: str):
        self.put_calls.append((Bucket, Key, len(Body), ContentType))
        if self._should_raise:
            raise self._should_raise
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}


class _Body:
    def __init__(self, data: bytes):
        self._d = data

    def read(self) -> bytes:
        return self._d


class _NoSuchKey(Exception):
    def __init__(self):
        self.response = {"Error": {"Code": "NoSuchKey"}}


class _Boto:
    def __init__(self, s3_stub: _S3Recorder):
        self._s3 = s3_stub

    def client(self, name: str):
        assert name == "s3"
        return self._s3


def test_paths_and_policy_force_runs_crawler(monkeypatch) -> None:
    mod = _load_module()

    os.environ["ENVIRONMENT"] = "dev"
    os.environ["CURATED_BUCKET"] = "curated-bucket-dev"
    os.environ["ARTIFACTS_BUCKET"] = "artifacts-bucket-dev"

    # Provide a latest fingerprint in artifacts
    latest = {"columns": [{"name": "a", "type": "string"}], "hash": "h1"}
    s3 = _S3Recorder(latest)
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    resp = mod["lambda_handler"](
        {
            "domain": "market",
            "table_name": "prices",
            "catalog_update": "force",
        },
        None,
    )

    # Should request these paths
    get_keys = {key for (_b, key) in s3.get_calls}
    assert "market/prices/_schema/latest.json" in get_keys
    # current.json read attempted (not found is acceptable in this test)
    assert "market/prices/_schema/current.json" in get_keys

    assert resp["should_crawl"] is True
    # When changed and allowed, it should persist current.json
    put_keys = {key for (_b, key, _n, _ct) in s3.put_calls}
    assert "market/prices/_schema/current.json" in put_keys


def test_latest_missing_returns_schema_check_failed(monkeypatch) -> None:
    mod = _load_module()

    os.environ["ENVIRONMENT"] = "dev"
    os.environ["CURATED_BUCKET"] = "curated-bucket-dev"
    os.environ["ARTIFACTS_BUCKET"] = "artifacts-bucket-dev"

    s3 = _S3Recorder(latest=None)
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    resp = mod["lambda_handler"]({"domain": "market", "table_name": "prices"}, None)
    assert resp["schema_changed"] is False
    assert resp["error"]["code"] == "SCHEMA_CHECK_FAILED"


def test_on_schema_change_policy_no_change(monkeypatch) -> None:
    mod = _load_module()

    os.environ["ENVIRONMENT"] = "dev"
    os.environ["CURATED_BUCKET"] = "curated-bucket-dev"
    os.environ["ARTIFACTS_BUCKET"] = "artifacts-bucket-dev"

    latest = {"columns": [{"name": "a", "type": "string"}], "hash": "h1"}
    current = {"columns": [{"name": "a", "type": "string"}], "hash": "h1"}
    s3 = _S3Recorder(latest=latest, current=current)
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    resp = mod["lambda_handler"](
        {
            "domain": "market",
            "table_name": "prices",
            "catalog_update": "on_schema_change",
        },
        None,
    )
    assert resp["schema_changed"] is False
    assert resp["should_crawl"] is False


def _setup_env() -> None:
    """Setup common environment variables for tests"""
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["CURATED_BUCKET"] = "curated-bucket-dev"
    os.environ["ARTIFACTS_BUCKET"] = "artifacts-bucket-dev"


def test_schema_changed_when_current_missing(monkeypatch) -> None:
    """
    Given: latest 스키마는 존재하지만 current.json이 없음
    When: 스키마 체크를 실행하면
    Then: schema_changed=True를 반환해야 함
    """
    mod = _load_module()
    _setup_env()

    latest = {
        "columns": [{"name": "symbol", "type": "string"}],
        "hash": "abc123",
    }
    s3 = _S3Recorder(latest=latest, current=None)  # current missing
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    resp = mod["lambda_handler"](
        {
            "domain": "market",
            "table_name": "prices",
            "catalog_update": "on_schema_change",
        },
        None,
    )
    assert resp["schema_changed"] is True
    assert resp["should_crawl"] is True
    assert resp["current_hash"] is None
    assert resp["latest_hash"] == "abc123"

    # Should persist latest -> current
    put_keys = {key for (_b, key, _n, _ct) in s3.put_calls}
    assert "market/prices/_schema/current.json" in put_keys


def test_schema_changed_when_hashes_differ(monkeypatch) -> None:
    """
    Given: current와 latest 스키마의 해시가 다름
    When: 스키마 체크를 실행하면
    Then: schema_changed=True를 반환해야 함
    """
    mod = _load_module()
    _setup_env()

    latest = {
        "columns": [
            {"name": "symbol", "type": "string"},
            {"name": "price", "type": "decimal"},
        ],
        "hash": "new_hash",
    }
    current = {
        "columns": [{"name": "symbol", "type": "string"}],
        "hash": "old_hash",
    }
    s3 = _S3Recorder(latest=latest, current=current)
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    resp = mod["lambda_handler"](
        {
            "domain": "market",
            "table_name": "prices",
            "catalog_update": "on_schema_change",
        },
        None,
    )
    assert resp["schema_changed"] is True
    assert resp["should_crawl"] is True
    assert resp["current_hash"] == "old_hash"
    assert resp["latest_hash"] == "new_hash"


def test_catalog_update_never_policy(monkeypatch) -> None:
    """
    Given: catalog_update 정책이 'never'로 설정됨
    When: 스키마가 변경되더라도
    Then: should_crawl=False를 반환해야 함
    """
    mod = _load_module()
    _setup_env()

    latest = {
        "columns": [{"name": "new_column", "type": "string"}],
        "hash": "new_hash",
    }
    current = {
        "columns": [{"name": "old_column", "type": "string"}],
        "hash": "old_hash",
    }
    s3 = _S3Recorder(latest=latest, current=current)
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    resp = mod["lambda_handler"](
        {
            "domain": "market",
            "table_name": "prices",
            "catalog_update": "never",
        },
        None,
    )
    assert resp["schema_changed"] is True
    assert resp["should_crawl"] is False

    # Should not persist when policy is "never"
    put_keys = {key for (_b, key, _n, _ct) in s3.put_calls}
    assert len(put_keys) == 0


def test_catalog_update_force_policy(monkeypatch) -> None:
    """
    Given: catalog_update 정책이 'force'로 설정됨
    When: 스키마가 변경되지 않더라도
    Then: should_crawl=True를 반환해야 함
    """
    mod = _load_module()
    _setup_env()

    # Same hash - no change
    latest = {
        "columns": [{"name": "symbol", "type": "string"}],
        "hash": "same_hash",
    }
    current = {
        "columns": [{"name": "symbol", "type": "string"}],
        "hash": "same_hash",
    }
    s3 = _S3Recorder(latest=latest, current=current)
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    resp = mod["lambda_handler"](
        {
            "domain": "market",
            "table_name": "prices",
            "catalog_update": "force",
        },
        None,
    )
    assert resp["schema_changed"] is False
    assert resp["should_crawl"] is True  # Force policy overrides


def test_schema_check_failed_missing_domain(monkeypatch) -> None:
    """
    Given: domain 필드가 누락됨
    When: 스키마 체크를 실행하면
    Then: PRE_VALIDATION_FAILED 에러를 반환해야 함
    """
    mod = _load_module()
    _setup_env()

    resp = mod["lambda_handler"]({"table_name": "prices"}, None)
    assert resp["schema_changed"] is False
    assert resp["should_crawl"] is False
    assert resp["error"]["code"] == "PRE_VALIDATION_FAILED"
    assert "domain/table_name required" in resp["error"]["message"]


def test_schema_check_failed_missing_table_name(monkeypatch) -> None:
    """
    Given: table_name 필드가 누락됨
    When: 스키마 체크를 실행하면
    Then: PRE_VALIDATION_FAILED 에러를 반환해야 함
    """
    mod = _load_module()
    _setup_env()

    resp = mod["lambda_handler"]({"domain": "market"}, None)
    assert resp["schema_changed"] is False
    assert resp["should_crawl"] is False
    assert resp["error"]["code"] == "PRE_VALIDATION_FAILED"
    assert "domain/table_name required" in resp["error"]["message"]


def test_schema_check_failed_missing_bucket_env() -> None:
    """
    Given: 필수 환경변수(CURATED_BUCKET, ARTIFACTS_BUCKET)가 누락됨
    When: 스키마 체크를 실행하면
    Then: PRE_VALIDATION_FAILED 에러를 반환해야 함
    """
    mod = _load_module()
    # Don't setup environment

    resp = mod["lambda_handler"]({"domain": "market", "table_name": "prices"}, None)
    assert resp["schema_changed"] is False
    assert resp["should_crawl"] is False
    assert resp["error"]["code"] == "PRE_VALIDATION_FAILED"
    assert "bucket env missing" in resp["error"]["message"]


def test_stable_hash_function_consistency(monkeypatch) -> None:
    """
    Given: 동일한 스키마 구조
    When: stable_hash 함수를 사용하면
    Then: 항상 동일한 해시를 생성해야 함
    """
    mod = _load_module()
    _setup_env()

    # Test stable hash function directly
    schema1 = {
        "columns": [
            {"name": "a", "type": "string"},
            {"name": "b", "type": "int"},
        ]
    }
    schema2 = {
        "columns": [
            {"name": "b", "type": "int"},
            {"name": "a", "type": "string"},
        ]
    }  # Different order

    hash1 = mod["_stable_hash"](schema1)
    hash2 = mod["_stable_hash"](schema2)

    # Should be same due to sort_keys=True
    assert hash1 == hash2
    assert isinstance(hash1, str)
    assert len(hash1) == 64  # SHA256 hex string


def test_hash_comparison_with_explicit_hash_field(monkeypatch) -> None:
    """
    Given: 스키마에 명시적인 'hash' 필드가 있음
    When: 스키마 비교를 수행하면
    Then: 명시적 해시를 우선 사용해야 함
    """
    mod = _load_module()
    _setup_env()

    latest = {
        "columns": [{"name": "symbol", "type": "string"}],
        "hash": "explicit_hash_123",
    }
    current = {
        "columns": [{"name": "symbol", "type": "string"}],
        "hash": "explicit_hash_456",
    }  # Different explicit hash
    s3 = _S3Recorder(latest=latest, current=current)
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    resp = mod["lambda_handler"]({"domain": "market", "table_name": "prices"}, None)
    assert resp["schema_changed"] is True
    assert resp["current_hash"] == "explicit_hash_456"
    assert resp["latest_hash"] == "explicit_hash_123"


def test_hash_comparison_fallback_to_computed_hash(monkeypatch) -> None:
    """
    Given: 스키마에 명시적인 'hash' 필드가 없음
    When: 스키마 비교를 수행하면
    Then: 구조를 기반으로 해시를 계산해야 함
    """
    mod = _load_module()
    _setup_env()

    latest = {"columns": [{"name": "symbol", "type": "string"}]}  # No explicit hash
    current = {"columns": [{"name": "symbol", "type": "string"}]}  # Same structure
    s3 = _S3Recorder(latest=latest, current=current)
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    resp = mod["lambda_handler"]({"domain": "market", "table_name": "prices"}, None)
    assert resp["schema_changed"] is False
    # Both hashes should be computed and identical
    assert resp["current_hash"] == resp["latest_hash"]


def test_catalog_update_policy_default_from_env(monkeypatch) -> None:
    """
    Given: catalog_update가 명시되지 않았지만 환경변수에 기본값이 설정됨
    When: 스키마 체크를 실행하면
    Then: 환경변수의 기본값을 사용해야 함
    """
    mod = _load_module()
    _setup_env()
    os.environ["CATALOG_UPDATE_DEFAULT"] = "force"

    latest = {
        "columns": [{"name": "symbol", "type": "string"}],
        "hash": "same_hash",
    }
    current = {
        "columns": [{"name": "symbol", "type": "string"}],
        "hash": "same_hash",
    }
    s3 = _S3Recorder(latest=latest, current=current)
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    resp = mod["lambda_handler"]({"domain": "market", "table_name": "prices"}, None)  # No catalog_update specified
    assert resp["schema_changed"] is False
    assert resp["should_crawl"] is True  # Should use "force" from env var


def test_s3_get_error_handling(monkeypatch) -> None:
    """
    Given: S3 get_object 호출 시 예외가 발생함
    When: 스키마 체크를 실행하면
    Then: 적절한 오류 처리를 해야 함
    """
    mod = _load_module()
    _setup_env()

    # Mock S3 error that's not NoSuchKey
    class _AccessDeniedError(Exception):
        def __init__(self):
            self.response = {"Error": {"Code": "AccessDenied"}}

    s3 = _S3Recorder(latest=None, should_raise=_AccessDeniedError())
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    # Should raise the exception since it's not NoSuchKey/404
    with pytest.raises(_AccessDeniedError):
        mod["lambda_handler"]({"domain": "market", "table_name": "prices"}, None)


def test_s3_put_operation_called_with_correct_parameters(monkeypatch) -> None:
    """
    Given: 스키마가 변경되고 정책이 허용함
    When: current.json을 업데이트할 때
    Then: 올바른 S3 put 파라미터를 사용해야 함
    """
    mod = _load_module()
    _setup_env()

    latest = {
        "columns": [{"name": "new_col", "type": "string"}],
        "hash": "new_hash",
    }
    current = {
        "columns": [{"name": "old_col", "type": "string"}],
        "hash": "old_hash",
    }
    s3 = _S3Recorder(latest=latest, current=current)
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    resp = mod["lambda_handler"](
        {
            "domain": "customer-data",
            "table_name": "orders",
            "catalog_update": "on_schema_change",
        },
        None,
    )

    assert resp["should_crawl"] is True

    # Verify put_object was called with correct parameters
    assert len(s3.put_calls) == 1
    bucket, key, body_length, content_type = s3.put_calls[0]
    assert bucket == "curated-bucket-dev"
    assert key == "customer-data/orders/_schema/current.json"
    assert content_type == "application/json"
    assert body_length > 0


@pytest.mark.parametrize(
    "policy,schema_changed,expected_crawl",
    [
        ("on_schema_change", True, True),
        ("on_schema_change", False, False),
        ("force", True, True),
        ("force", False, True),
        ("never", True, False),
        ("never", False, False),
        ("invalid_policy", True, True),  # Falls back to on_schema_change
        ("invalid_policy", False, False),  # Falls back to on_schema_change
    ],
)
def test_catalog_update_policy_matrix(monkeypatch, policy, schema_changed, expected_crawl) -> None:
    """
    Given: 다양한 카탈로그 업데이트 정책과 스키마 변경 상태
    When: 스키마 체크를 실행하면
    Then: 정책에 따른 올바른 크롤링 결정을 해야 함
    """
    mod = _load_module()
    _setup_env()

    if schema_changed:
        latest = {
            "columns": [{"name": "new_col", "type": "string"}],
            "hash": "new_hash",
        }
        current = {
            "columns": [{"name": "old_col", "type": "string"}],
            "hash": "old_hash",
        }
    else:
        latest = {
            "columns": [{"name": "same_col", "type": "string"}],
            "hash": "same_hash",
        }
        current = {
            "columns": [{"name": "same_col", "type": "string"}],
            "hash": "same_hash",
        }

    s3 = _S3Recorder(latest=latest, current=current)
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    resp = mod["lambda_handler"](
        {"domain": "market", "table_name": "prices", "catalog_update": policy},
        None,
    )

    assert resp["schema_changed"] == schema_changed
    assert resp["should_crawl"] == expected_crawl


def test_edge_case_empty_schema_structures(monkeypatch) -> None:
    """
    Given: 빈 스키마 구조들
    When: 스키마 체크를 실행하면
    Then: 적절히 처리해야 함
    """
    mod = _load_module()
    _setup_env()

    # Empty columns array
    latest = {"columns": [], "hash": "empty_latest"}
    current = {"columns": [], "hash": "empty_current"}
    s3 = _S3Recorder(latest=latest, current=current)
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto(s3).client)

    resp = mod["lambda_handler"]({"domain": "market", "table_name": "prices"}, None)
    assert resp["schema_changed"] is True  # Different hashes
    assert resp["current_hash"] == "empty_current"
    assert resp["latest_hash"] == "empty_latest"
