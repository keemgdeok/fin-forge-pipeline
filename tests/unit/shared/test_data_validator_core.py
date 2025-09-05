from botocore.exceptions import ClientError
import shared.validation.data_validator as dv


class _S3OK:
    def head_object(self, **kwargs):
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}


class _S3Fail:
    def head_object(self, **kwargs):
        raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")


class _Boto:
    def __init__(self, s3):
        self._s3 = s3

    def client(self, name: str, region_name: str | None = None):
        if name == "s3":
            return self._s3

        # Unused clients in these tests
        class _Stub:
            pass

        return _Stub()


def test_validate_file_type_allowed(monkeypatch):
    # Patch boto3 in module to avoid real AWS calls
    monkeypatch.setitem(dv.__dict__, "boto3", _Boto(_S3OK()))
    validator = dv.DataValidator()
    res = validator.validate_data_comprehensive(
        {"source_bucket": "b", "source_key": "k", "file_type": "csv", "validation_rules": {"rule": True}}
    )
    assert res["overall_valid"] is True


def test_validate_file_type_rejected(monkeypatch):
    monkeypatch.setitem(dv.__dict__, "boto3", _Boto(_S3OK()))
    validator = dv.DataValidator()
    res = validator.validate_data_comprehensive(
        {"source_bucket": "b", "source_key": "k", "file_type": "xlsx", "validation_rules": {"rule": True}}
    )
    assert res["overall_valid"] is False
    assert any(e.get("type") == "InvalidFileType" for e in res["errors"])  # type: ignore[arg-type]


def test_validate_s3_head_failure(monkeypatch):
    monkeypatch.setitem(dv.__dict__, "boto3", _Boto(_S3Fail()))
    validator = dv.DataValidator()
    res = validator.validate_data_comprehensive(
        {"source_bucket": "b", "source_key": "k", "file_type": "csv", "validation_rules": {"rule": True}}
    )
    assert res["overall_valid"] is False
    assert any(e.get("type") == "S3AccessError" for e in res["errors"])  # type: ignore[arg-type]
