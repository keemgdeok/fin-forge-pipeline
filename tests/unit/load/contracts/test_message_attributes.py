import os
import pytest
from typing import Any, Dict


pytestmark = [pytest.mark.unit, pytest.mark.load]


TARGET = "src/lambda/shared/layers/core/load_contracts.py"


if not os.path.exists(TARGET):
    pytest.skip("Load contracts module not yet implemented", allow_module_level=True)


def test_build_message_attributes(load_module) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    LoadMessage = mod["LoadMessage"]
    build_message_attributes = mod["build_message_attributes"]

    # Given: market 도메인의 LoadMessage가 있고
    msg = LoadMessage(
        bucket="data-pipeline-curated-dev",
        key="market/prices/ds=2025-09-10/part-001.parquet",
        domain="market",
        table_name="prices",
        partition="ds=2025-09-10",
        correlation_id="550e8400-e29b-41d4-a716-446655440000",
    )

    # When: 메시지 속성을 생성하면
    # Then: ContentType과 우선순위가 스펙과 일치한다
    attrs = build_message_attributes(msg)
    assert attrs["ContentType"] == "application/json"
    assert attrs["Domain"] == "market"
    assert attrs["TableName"] == "prices"
    # Market domain is high priority per spec -> priority 1
    assert attrs["Priority"] == "1"


@pytest.mark.parametrize(
    "domain,expected",
    [
        ("market", "1"),
        ("customer", "2"),
        ("product", "3"),
        ("analytics", "3"),
        ("unknown", "3"),
    ],
)
def test_priority_mapping(load_module, domain: str, expected: str) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    LoadMessage = mod["LoadMessage"]
    build_message_attributes = mod["build_message_attributes"]

    # Given: 다양한 도메인의 LoadMessage가 주어지고
    # When: 메시지 속성을 생성하면
    # Then: Priority 값이 도메인 규칙에 맞게 계산된다
    msg = LoadMessage(
        bucket="data-pipeline-curated-dev",
        key=f"{domain}/prices/ds=2025-09-10/part-001.parquet",
        domain=domain,
        table_name="prices",
        partition="ds=2025-09-10",
        correlation_id="550e8400-e29b-41d4-a716-446655440000",
    )

    attrs = build_message_attributes(msg)
    assert attrs["Priority"] == expected
