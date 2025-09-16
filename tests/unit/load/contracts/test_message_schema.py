import os
import pytest
from typing import Any, Dict


pytestmark = [pytest.mark.unit, pytest.mark.load]


TARGET = "src/lambda/shared/layers/core/load_contracts.py"


if not os.path.exists(TARGET):
    pytest.skip("Load contracts module not yet implemented", allow_module_level=True)


def test_valid_message_schema(load_module) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    LoadMessage = mod["LoadMessage"]

    # Given: 유효한 LoadMessage 필드값 세트가 있고
    msg = LoadMessage(
        bucket="data-pipeline-curated-dev",
        key="market/prices/ds=2025-09-10/part-001.parquet",
        domain="market",
        table_name="prices",
        partition="ds=2025-09-10",
        correlation_id="550e8400-e29b-41d4-a716-446655440000",
        file_size=1048576,
        presigned_url="https://example.com/presigned",
    )

    # When: LoadMessage 인스턴스를 생성하면
    # Then: 각 속성이 기대값과 정확히 일치한다
    assert msg.bucket == "data-pipeline-curated-dev"
    assert msg.domain == "market"
    assert msg.table_name == "prices"
    assert msg.partition == "ds=2025-09-10"
    assert msg.file_size == 1048576
    assert msg.presigned_url == "https://example.com/presigned"


def test_invalid_partition_format(load_module) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    LoadMessage = mod["LoadMessage"]
    ValidationError = mod.get("ValidationError")

    # Given: 잘못된 파티션 포맷을 가진 입력이 있고
    # When: LoadMessage를 생성하면
    # Then: ValidationError가 발생한다
    with pytest.raises(ValidationError or Exception):  # type: ignore[arg-type]
        LoadMessage(
            bucket="b",
            key="market/prices/not-a-ds/part.parquet",
            domain="market",
            table_name="prices",
            partition="not-a-ds",
            correlation_id="550e8400-e29b-41d4-a716-446655440000",
        )


def test_correlation_id_uuid_v4(load_module) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    LoadMessage = mod["LoadMessage"]
    ValidationError = mod.get("ValidationError")

    # Given: UUID v4 형식이 아닌 correlation_id가 주어지고
    # When: LoadMessage를 생성하려고 하면
    # Then: ValidationError가 발생한다
    with pytest.raises(ValidationError or Exception):  # type: ignore[arg-type]
        LoadMessage(
            bucket="b",
            key="market/prices/ds=2025-09-10/part.parquet",
            domain="market",
            table_name="prices",
            partition="ds=2025-09-10",
            correlation_id="not-a-uuid",
        )


def test_invalid_bucket_and_table_formats(load_module) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    LoadMessage = mod["LoadMessage"]
    ValidationError = mod.get("ValidationError")

    # Given: 버킷/도메인/테이블 포맷이 스펙을 위반하고
    # When: LoadMessage를 생성하면
    # Then: ValidationError가 발생한다
    with pytest.raises(ValidationError or Exception):  # type: ignore[arg-type]
        LoadMessage(
            bucket="INVALID_BUCKET",
            key="market/prices/ds=2025-09-10/part-001.txt",
            domain="market-domain",
            table_name="bad-table!",
            partition="ds=2025-09-10",
            correlation_id="550e8400-e29b-41d4-a716-446655440000",
        )


def test_file_size_must_be_positive(load_module) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    LoadMessage = mod["LoadMessage"]
    ValidationError = mod.get("ValidationError")

    # Given: file_size 값이 0으로 주어지고
    # When: LoadMessage를 생성하려고 하면
    # Then: ValidationError가 발생한다
    with pytest.raises(ValidationError or Exception):  # type: ignore[arg-type]
        LoadMessage(
            bucket="data-pipeline-curated-dev",
            key="market/prices/ds=2025-09-10/part-001.parquet",
            domain="market",
            table_name="prices",
            partition="ds=2025-09-10",
            correlation_id="550e8400-e29b-41d4-a716-446655440000",
            file_size=0,
        )


def test_domain_and_partition_must_match_key(load_module) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    LoadMessage = mod["LoadMessage"]
    ValidationError = mod.get("ValidationError")

    # Given: S3 키와 입력된 도메인이 일치하지 않고
    # When: LoadMessage를 생성하면
    # Then: ValidationError가 발생한다
    with pytest.raises(ValidationError or Exception):  # type: ignore[arg-type]
        LoadMessage(
            bucket="data-pipeline-curated-dev",
            key="market/prices/ds=2025-09-10/part-001.parquet",
            domain="customer",
            table_name="prices",
            partition="ds=2025-09-10",
            correlation_id="550e8400-e29b-41d4-a716-446655440000",
        )
