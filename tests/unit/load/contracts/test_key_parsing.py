import os
import pytest
from typing import Any, Dict


pytestmark = [pytest.mark.unit, pytest.mark.load]


TARGET = "src/lambda/layers/load/contracts/python/load_contracts.py"


if not os.path.exists(TARGET):
    pytest.skip("Load contracts module not yet implemented", allow_module_level=True)


def test_parse_curated_key_success(load_module) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    parse_curated_key = mod["parse_curated_key"]

    # Given: 스펙을 만족하는 S3 키가 주어지고
    # When: parse_curated_key를 호출하면
    # Then: 도메인/테이블/파티션 값이 올바르게 파싱된다
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
    mod: Dict[str, Any] = load_module(TARGET)
    parse_curated_key = mod["parse_curated_key"]

    # Given: 잘못된 파티션 세그먼트를 가진 키가 있고
    # When: parse_curated_key를 호출하면
    # Then: ValueError가 발생한다
    with pytest.raises(ValueError):
        parse_curated_key("market/prices/not-a-valid-partition/file.parquet")
