import pytest
from typing import Any, Dict


pytestmark = [pytest.mark.unit, pytest.mark.load]


TARGET = "src/lambda/layers/load/contracts/python/load_contracts.py"


def test_build_message_attributes(load_module) -> None:
    """
    Given: market 도메인의 LoadMessage
    When: build_message_attributes 호출
    Then: ContentType과 Priority가 스펙과 일치
    """
    mod: Dict[str, Any] = load_module(TARGET)
    LoadMessage = mod["LoadMessage"]
    build_message_attributes = mod["build_message_attributes"]

    msg = LoadMessage(
        bucket="data-pipeline-curated-dev",
        key="market/prices/interval=1d/data_source=yahoo/year=2025/month=09/day=10/layer=adjusted/part-001.parquet",
        domain="market",
        table_name="prices",
        interval="1d",
        data_source="yahoo",
        year="2025",
        month="09",
        day="10",
        layer="adjusted",
        ds="2025-09-10",
        correlation_id="550e8400-e29b-41d4-a716-446655440000",
    )

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
        ("daily-prices-data", "2"),
        ("product", "3"),
        ("analytics", "3"),
        ("unknown", "3"),
    ],
)
def test_priority_mapping(load_module, domain: str, expected: str) -> None:
    """
    Given: 도메인별 LoadMessage
    When: build_message_attributes 호출
    Then: Priority가 도메인 규칙과 일치
    """
    mod: Dict[str, Any] = load_module(TARGET)
    LoadMessage = mod["LoadMessage"]
    build_message_attributes = mod["build_message_attributes"]

    msg = LoadMessage(
        bucket="data-pipeline-curated-dev",
        key=(
            f"{domain}/prices/interval=1d/data_source=yahoo/year=2025/month=09/day=10/layer=adjusted/part-001.parquet"
        ),
        domain=domain,
        table_name="prices",
        interval="1d",
        data_source="yahoo",
        year="2025",
        month="09",
        day="10",
        layer="adjusted",
        ds="2025-09-10",
        correlation_id="550e8400-e29b-41d4-a716-446655440000",
    )

    attrs = build_message_attributes(msg)
    assert attrs["Priority"] == expected
