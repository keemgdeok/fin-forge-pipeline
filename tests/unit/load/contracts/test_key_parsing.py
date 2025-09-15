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

    domain, table, partition = parse_curated_key("market/prices/ds=2025-09-10/part-0001.parquet")
    assert domain == "market"
    assert table == "prices"
    assert partition == "ds=2025-09-10"


def test_parse_curated_key_invalid(load_module) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    parse_curated_key = mod["parse_curated_key"]

    with pytest.raises(ValueError):
        parse_curated_key("market/prices/not-a-ds/file.parquet")
