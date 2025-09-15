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

    msg = LoadMessage(
        bucket="data-pipeline-curated-dev",
        key="market/prices/ds=2025-09-10/part-001.parquet",
        domain="market",
        table_name="prices",
        partition="ds=2025-09-10",
        correlation_id="550e8400-e29b-41d4-a716-446655440000",
        file_size=1048576,
    )

    assert msg.bucket == "data-pipeline-curated-dev"
    assert msg.domain == "market"
    assert msg.table_name == "prices"
    assert msg.partition == "ds=2025-09-10"


def test_invalid_partition_format(load_module) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    LoadMessage = mod["LoadMessage"]
    ValidationError = mod.get("ValidationError")

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

    with pytest.raises(ValidationError or Exception):  # type: ignore[arg-type]
        LoadMessage(
            bucket="b",
            key="market/prices/ds=2025-09-10/part.parquet",
            domain="market",
            table_name="prices",
            partition="ds=2025-09-10",
            correlation_id="not-a-uuid",
        )
