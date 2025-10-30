import pytest
from typing import Any, Dict

from tests.fixtures.load_builders import build_s3_object_created_event


pytestmark = [pytest.mark.unit, pytest.mark.infrastructure, pytest.mark.load]


TARGET = "src/lambda/layers/load/contracts/python/load_contracts.py"


def test_event_transform_to_sqs_message(load_module) -> None:
    """
    Given: 유효한 S3 Object Created 이벤트
    When: transform_s3_event_to_message 호출
    Then: 메시지 필드가 스펙에 맞게 생성
    """
    mod: Dict[str, Any] = load_module(TARGET)
    transform = mod["transform_s3_event_to_message"]

    bucket = "data-pipeline-curated-dev"
    key = "market/prices/interval=1d/data_source=yahoo/year=2025/month=09/day=10/layer=adjusted/part-001.parquet"
    event = build_s3_object_created_event(bucket=bucket, key=key, size=1337)

    out = transform(event)
    assert out["bucket"] == bucket
    assert out["key"] == key
    assert out["domain"] == "market"
    assert out["table_name"] == "prices"
    assert out["interval"] == "1d"
    assert out["data_source"] == "yahoo"
    assert out["year"] == "2025"
    assert out["month"] == "09"
    assert out["day"] == "10"
    assert out["layer"] == "adjusted"
    assert out["ds"] == "2025-09-10"
    assert out.get("file_size") == 1337
    assert "correlation_id" in out


def test_event_transform_rejects_small_or_invalid_files(load_module) -> None:
    """
    Given: 확장자 또는 크기가 유효하지 않은 이벤트
    When: transform_s3_event_to_message 호출
    Then: ValidationError 발생
    """
    mod: Dict[str, Any] = load_module(TARGET)
    transform = mod["transform_s3_event_to_message"]
    ValidationError = mod.get("ValidationError")

    bucket = "data-pipeline-curated-dev"
    bad_key = "market/prices/interval=1d/data_source=yahoo/year=2025/month=09/day=10/layer=adjusted/part-001.csv"
    bad_event = build_s3_object_created_event(bucket=bucket, key=bad_key, size=1337)

    with pytest.raises(ValidationError or Exception):  # type: ignore[arg-type]
        transform(bad_event)

    valid_key = "market/prices/interval=1d/data_source=yahoo/year=2025/month=09/day=10/layer=adjusted/part-001.parquet"
    tiny_event = build_s3_object_created_event(bucket=bucket, key=valid_key, size=1)

    with pytest.raises(ValidationError or Exception):  # type: ignore[arg-type]
        transform(tiny_event)


def test_event_transform_rejects_invalid_event_type(load_module) -> None:
    """
    Given: 지원되지 않는 source를 가진 이벤트
    When: transform_s3_event_to_message 호출
    Then: ValidationError 발생
    """
    mod: Dict[str, Any] = load_module(TARGET)
    transform = mod["transform_s3_event_to_message"]
    ValidationError = mod.get("ValidationError")

    event = build_s3_object_created_event(
        bucket="data-pipeline-curated-dev",
        key="market/prices/interval=1d/data_source=yahoo/year=2025/month=09/day=10/layer=adjusted/part.parquet",
        size=2048,
    )
    event["source"] = "custom.source"

    with pytest.raises(ValidationError or Exception):  # type: ignore[arg-type]
        transform(event)


def test_event_transform_requires_object_payload(load_module) -> None:
    """
    Given: object 세부 정보가 비어 있는 이벤트
    When: transform_s3_event_to_message 호출
    Then: ValidationError 발생
    """
    mod: Dict[str, Any] = load_module(TARGET)
    transform = mod["transform_s3_event_to_message"]
    ValidationError = mod.get("ValidationError")

    event: Dict[str, Any] = {
        "source": "aws.s3",
        "detail-type": "Object Created",
        "detail": {},
    }

    with pytest.raises(ValidationError or Exception):  # type: ignore[arg-type]
        transform(event)
