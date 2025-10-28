import pytest
from typing import Any, Dict


pytestmark = [pytest.mark.unit, pytest.mark.load]


TARGET = "src/lambda/layers/load/contracts/python/load_contracts.py"


def test_parse_curated_key_success(load_module) -> None:
    """
    Given: 스펙을 만족하는 S3 키
    When: parse_curated_key 호출
    Then: 도메인과 파티션이 정확히 파싱
    """
    mod: Dict[str, Any] = load_module(TARGET)
    parse_curated_key = mod["parse_curated_key"]

    key = "market/prices/interval=1d/data_source=yahoo/year=2025/month=09/day=10/layer=adjusted/part-0001.parquet"
    result = parse_curated_key(key)
    assert result.domain == "market"
    assert result.table_name == "prices"
    assert result.interval == "1d"
    assert result.data_source == "yahoo"
    assert result.year == "2025"
    assert result.month == "09"
    assert result.day == "10"
    assert result.layer == "adjusted"
    assert result.ds == "2025-09-10"


def test_parse_curated_key_invalid(load_module) -> None:
    """
    Given: 파티션 세그먼트가 잘못된 키
    When: parse_curated_key 호출
    Then: ValueError 발생
    """
    mod: Dict[str, Any] = load_module(TARGET)
    parse_curated_key = mod["parse_curated_key"]

    with pytest.raises(ValueError):
        parse_curated_key("market/prices/not-a-valid-partition/file.parquet")
