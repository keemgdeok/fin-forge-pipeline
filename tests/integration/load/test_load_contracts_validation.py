from __future__ import annotations

import pytest
from moto import mock_aws

from tests.integration.load import helpers

pytestmark = [pytest.mark.integration, pytest.mark.load]


def _build_valid_payload(**overrides):
    payload = helpers.build_message(
        part="part-valid.parquet",
        correlation_id="550e8400-e29b-41d4-a716-446655440000",
    )
    payload.update(overrides)
    payload.setdefault("presigned_url", "https://example.com/object")
    payload.setdefault("file_size", 1024)
    return payload


@mock_aws
def test_load_message_accepts_optional_fields(load_module) -> None:
    """
    Given: file_size와 presigned_url이 포함된 유효한 메시지
    When: LoadMessage를 생성
    Then: to_dict 결과에 선택적 필드가 포함되어야 함
    """
    module = load_module("src/lambda/layers/load/contracts/python/load_contracts.py")
    LoadMessage = module["LoadMessage"]

    message = LoadMessage(**_build_valid_payload())
    as_dict = message.to_dict()
    assert as_dict["file_size"] == 1024
    assert as_dict["presigned_url"].startswith("https://")
