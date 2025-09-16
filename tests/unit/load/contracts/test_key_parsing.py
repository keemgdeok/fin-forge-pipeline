import os
import pytest
from typing import Any, Dict


pytestmark = [pytest.mark.unit, pytest.mark.load]


TARGET = "src/lambda/shared/layers/core/load_contracts.py"


if not os.path.exists(TARGET):
    pytest.skip("Load contracts module not yet implemented", allow_module_level=True)


def test_parse_curated_key_success(load_module) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    parse_curated_key = mod["parse_curated_key"]

    # Given: 스펙을 만족하는 S3 키가 주어지고
    # When: parse_curated_key를 호출하면
    # Then: 도메인/테이블/파티션이 올바르게 파싱된다
    domain, table, partition = parse_curated_key("market/prices/ds=2025-09-10/part-0001.parquet")
    assert domain == "market"
    assert table == "prices"
    assert partition == "ds=2025-09-10"


def test_parse_curated_key_invalid(load_module) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    parse_curated_key = mod["parse_curated_key"]

    # Given: 잘못된 파티션 세그먼트를 가진 키가 있고
    # When: parse_curated_key를 호출하면
    # Then: ValueError가 발생한다
    with pytest.raises(ValueError):
        parse_curated_key("market/prices/not-a-ds/file.parquet")
