import pytest
from typing import Any, Dict


pytestmark = [pytest.mark.unit, pytest.mark.load]


TARGET = "src/lambda/layers/load/contracts/python/load_contracts.py"
VALID_KEY = "market/prices/interval=1d/data_source=yahoo/year=2025/month=09/day=10/layer=adjusted/part-001.parquet"
VALID_CORRELATION_ID = "550e8400-e29b-41d4-a716-446655440000"


def _payload(**overrides: Any) -> Dict[str, Any]:
    base: Dict[str, Any] = {
        "bucket": "data-pipeline-curated-dev",
        "key": VALID_KEY,
        "domain": "market",
        "table_name": "prices",
        "interval": "1d",
        "data_source": "yahoo",
        "year": "2025",
        "month": "09",
        "day": "10",
        "layer": "adjusted",
        "ds": "2025-09-10",
        "correlation_id": VALID_CORRELATION_ID,
    }
    base.update(overrides)
    return base


def test_valid_message_schema(load_module) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    LoadMessage = mod["LoadMessage"]

    # Given: 유효한 LoadMessage 필드값 세트가 있고
    msg = LoadMessage(**_payload(file_size=1048576, presigned_url="https://example.com/presigned"))

    # When: LoadMessage 인스턴스를 생성하면
    # Then: 각 속성이 기대값과 정확히 일치한다
    assert msg.bucket == "data-pipeline-curated-dev"
    assert msg.domain == "market"
    assert msg.table_name == "prices"
    assert msg.interval == "1d"
    assert msg.data_source == "yahoo"
    assert msg.layer == "adjusted"
    assert msg.ds == "2025-09-10"
    assert msg.file_size == 1048576
    assert msg.presigned_url == "https://example.com/presigned"


def test_invalid_date_segment(load_module) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    LoadMessage = mod["LoadMessage"]
    ValidationError = mod.get("ValidationError")

    # Given: 날짜 세그먼트가 잘못된 입력이 있고
    # When: LoadMessage를 생성하면
    # Then: ValidationError가 발생한다
    with pytest.raises(ValidationError or Exception):  # type: ignore[arg-type]
        LoadMessage(
            **_payload(
                bucket="b",
                key="market/prices/interval=1d/year=2025/month=09/day=10/layer=adjusted/part.parquet",
                data_source=None,
                month="13",
                ds="2025-13-10",
            )
        )


def test_correlation_id_uuid_v4(load_module) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    LoadMessage = mod["LoadMessage"]
    ValidationError = mod.get("ValidationError")

    # Given: UUID v4 형식이 아닌 correlation_id가 주어지고
    # When: LoadMessage를 생성하려고 하면
    # Then: ValidationError가 발생한다
    with pytest.raises(ValidationError or Exception):  # type: ignore[arg-type]
        LoadMessage(**_payload(bucket="b", correlation_id="not-a-uuid"))


def test_message_allows_missing_data_source(load_module) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    LoadMessage = mod["LoadMessage"]

    # Given: data_source 파티션이 없는 키가 있고
    msg = LoadMessage(
        **_payload(
            key="market/prices/interval=1d/year=2025/month=09/day=10/layer=adjusted/part-001.parquet",
            data_source=None,
        )
    )

    # Then: data_source는 None으로 유지되고 dict에서도 제외된다
    assert msg.data_source is None
    assert "data_source" not in msg.to_dict()


def test_presigned_url_must_be_https(load_module) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    LoadMessage = mod["LoadMessage"]
    ValidationError = mod.get("ValidationError")

    msg = LoadMessage(**_payload(presigned_url="https://example.com/object"))
    assert msg.presigned_url == "https://example.com/object"

    object.__setattr__(msg, "presigned_url", "http://example.com")
    with pytest.raises(ValidationError or Exception):  # type: ignore[arg-type]
        msg._validate_presigned_url()


def test_invalid_bucket_and_table_formats(load_module) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    LoadMessage = mod["LoadMessage"]
    ValidationError = mod.get("ValidationError")

    # Given: 버킷/도메인/테이블 포맷이 스펙을 위반하고
    # When: LoadMessage를 생성하면
    # Then: ValidationError가 발생한다
    with pytest.raises(ValidationError or Exception):  # type: ignore[arg-type]
        LoadMessage(
            **_payload(
                bucket="INVALID_BUCKET",
                key="market/prices/interval=1d/year=2025/month=09/day=10/layer=adjusted/part-001.txt",
                domain="market-domain",
                table_name="bad-table!",
                data_source=None,
            )
        )


def test_file_size_must_be_positive(load_module) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    LoadMessage = mod["LoadMessage"]
    ValidationError = mod.get("ValidationError")

    # Given: file_size 값이 0으로 주어지고
    # When: LoadMessage를 생성하려고 하면
    # Then: ValidationError가 발생한다
    with pytest.raises(ValidationError or Exception):  # type: ignore[arg-type]
        LoadMessage(**_payload(file_size=0))


def test_domain_and_partitions_must_match_key(load_module) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    LoadMessage = mod["LoadMessage"]
    ValidationError = mod.get("ValidationError")

    # Given: S3 키와 입력된 도메인이 일치하지 않고
    # When: LoadMessage를 생성하면
    # Then: ValidationError가 발생한다
    with pytest.raises(ValidationError or Exception):  # type: ignore[arg-type]
        LoadMessage(**_payload(domain="daily-prices-data"))


@pytest.mark.parametrize(
    "overrides",
    [
        {"table_name": "quotes"},
        {"interval": "1h"},
        {"layer": "technical_indicator"},
        {"year": "2024"},
        {"month": "10"},
        {"day": "11"},
        {"data_source": None},
        {"ds": "2025-09-11"},
    ],
)
def test_attribute_mismatch_against_key(load_module, overrides: Dict[str, Any]) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    LoadMessage = mod["LoadMessage"]
    ValidationError = mod.get("ValidationError")

    with pytest.raises(ValidationError or Exception):  # type: ignore[arg-type]
        LoadMessage(**_payload(**overrides))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("interval", "invalid interval"),
        ("interval", 123),
        ("data_source", "INVALID*VALUE"),
        ("data_source", 123),
        ("year", "20"),
        ("year", 2025),
        ("month", "13"),
        ("month", 9),
        ("day", "32"),
        ("day", 10),
        ("layer", "bad layer"),
        ("layer", 1),
        ("ds", "2025/09/10"),
        ("ds", 20250910),
    ],
)
def test_validate_fields_rejects_invalid_values(load_module, field: str, value: Any) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    LoadMessage = mod["LoadMessage"]
    ValidationError = mod.get("ValidationError")

    msg = LoadMessage(**_payload())

    object.__setattr__(msg, field, value)
    with pytest.raises(ValidationError or Exception):  # type: ignore[arg-type]
        msg._validate_fields()
