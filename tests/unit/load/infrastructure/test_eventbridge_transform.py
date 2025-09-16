import os
import pytest
from typing import Any, Dict

from tests.fixtures.load_builders import build_s3_object_created_event


pytestmark = [pytest.mark.unit, pytest.mark.infrastructure, pytest.mark.load]


TARGET = "src/lambda/shared/layers/core/load_contracts.py"


if not os.path.exists(TARGET):
    pytest.skip("Load contracts module not yet implemented", allow_module_level=True)


def test_event_transform_to_sqs_message(load_module) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    transform = mod["transform_s3_event_to_message"]

    bucket = "data-pipeline-curated-dev"
    key = "market/prices/ds=2025-09-10/part-001.parquet"
    event = build_s3_object_created_event(bucket=bucket, key=key, size=1337)

    out = transform(event)
    assert out["bucket"] == bucket
    assert out["key"] == key
    assert out["domain"] == "market"
    assert out["table_name"] == "prices"
    assert out["partition"] == "ds=2025-09-10"
    assert out.get("file_size") == 1337
    assert "correlation_id" in out


def test_event_transform_rejects_small_or_invalid_files(load_module) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    transform = mod["transform_s3_event_to_message"]
    ValidationError = mod.get("ValidationError")

    bucket = "data-pipeline-curated-dev"
    bad_key = "market/prices/ds=2025-09-10/part-001.csv"
    bad_event = build_s3_object_created_event(bucket=bucket, key=bad_key, size=1337)

    with pytest.raises(ValidationError or Exception):  # type: ignore[arg-type]
        transform(bad_event)

    tiny_event = build_s3_object_created_event(
        bucket=bucket, key="market/prices/ds=2025-09-10/part-001.parquet", size=1
    )

    with pytest.raises(ValidationError or Exception):  # type: ignore[arg-type]
        transform(tiny_event)
