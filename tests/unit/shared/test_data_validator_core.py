import shared.validation.data_validator as dv
from tests.fixtures.clients import S3Stub, BotoStub


def test_validate_file_type_allowed(monkeypatch):
    """
    Given: S3 head 성공 스텁과 허용된 파일 타입(csv)
    When: 종합 검증을 수행하면
    Then: overall_valid가 True여야 함
    """
    # Patch boto3 in module to avoid real AWS calls
    monkeypatch.setitem(dv.__dict__, "boto3", BotoStub(s3=S3Stub(head_ok=True)))
    validator = dv.DataValidator()
    res = validator.validate_data_comprehensive(
        {"source_bucket": "b", "source_key": "k", "file_type": "csv", "validation_rules": {"rule": True}}
    )
    assert res["overall_valid"] is True


def test_validate_file_type_rejected(monkeypatch):
    """
    Given: S3 head 성공 스텁과 허용되지 않은 파일 타입(xlsx)
    When: 종합 검증을 수행하면
    Then: overall_valid가 False이고 InvalidFileType 오류가 포함되어야 함
    """
    monkeypatch.setitem(dv.__dict__, "boto3", BotoStub(s3=S3Stub(head_ok=True)))
    validator = dv.DataValidator()
    res = validator.validate_data_comprehensive(
        {"source_bucket": "b", "source_key": "k", "file_type": "xlsx", "validation_rules": {"rule": True}}
    )
    assert res["overall_valid"] is False
    assert any(e.get("type") == "InvalidFileType" for e in res["errors"])  # type: ignore[arg-type]


def test_validate_s3_head_failure(monkeypatch):
    """
    Given: S3 head 404 실패 스텁과 허용된 파일 타입(csv)
    When: 종합 검증을 수행하면
    Then: overall_valid가 False이고 S3AccessError 오류가 포함되어야 함
    """
    monkeypatch.setitem(dv.__dict__, "boto3", BotoStub(s3=S3Stub(head_ok=False)))
    validator = dv.DataValidator()
    res = validator.validate_data_comprehensive(
        {"source_bucket": "b", "source_key": "k", "file_type": "csv", "validation_rules": {"rule": True}}
    )
    assert res["overall_valid"] is False
    assert any(e.get("type") == "S3AccessError" for e in res["errors"])  # type: ignore[arg-type]
