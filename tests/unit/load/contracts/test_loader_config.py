import os
import pytest
from typing import Any, Dict


pytestmark = [pytest.mark.unit, pytest.mark.load]


TARGET = "src/lambda/shared/layers/core/load_contracts.py"


if not os.path.exists(TARGET):
    pytest.skip("Load contracts module not yet implemented", allow_module_level=True)


def test_loader_config_defaults(load_module) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    LoaderConfig = mod["LoaderConfig"]

    # Given: 최소 필수 입력으로 LoaderConfig를 생성하고
    cfg = LoaderConfig(queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/dev-domain-load-queue")
    # When: 기본 속성을 확인하면
    # Then: 스펙에서 정의한 기본값을 따르는지 검증한다
    assert cfg.wait_time_seconds == 20
    assert cfg.max_messages == 10
    assert cfg.visibility_timeout == 1800
    assert cfg.query_timeout == 300
    assert list(cfg.backoff_seconds) == [2, 4, 8]


@pytest.mark.parametrize(
    "field,kwargs",
    [
        ("queue_url", {"queue_url": ""}),
        (
            "wait_time_seconds",
            {
                "queue_url": "https://sqs.us-east-1.amazonaws.com/123456789012/dev-domain-load-queue",
                "wait_time_seconds": 0,
            },
        ),
        (
            "max_messages",
            {"queue_url": "https://sqs.us-east-1.amazonaws.com/123456789012/dev-domain-load-queue", "max_messages": 0},
        ),
        (
            "visibility_timeout",
            {
                "queue_url": "https://sqs.us-east-1.amazonaws.com/123456789012/dev-domain-load-queue",
                "visibility_timeout": 100,
            },
        ),
        (
            "query_timeout",
            {"queue_url": "https://sqs.us-east-1.amazonaws.com/123456789012/dev-domain-load-queue", "query_timeout": 0},
        ),
    ],
)
def test_loader_config_validation(load_module, field: str, kwargs: Dict[str, Any]) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    LoaderConfig = mod["LoaderConfig"]
    ValidationError = mod.get("ValidationError")

    # Given: 각 필드를 스펙에 어긋나게 설정하고
    # When: LoaderConfig를 생성하면
    # Then: ValidationError가 발생한다
    base = {"queue_url": "https://sqs.us-east-1.amazonaws.com/123456789012/dev-domain-load-queue"}
    base.update(kwargs)

    with pytest.raises(ValidationError or Exception):  # type: ignore[arg-type]
        LoaderConfig(**base)


def test_loader_config_visibility_multiple(load_module) -> None:
    mod: Dict[str, Any] = load_module(TARGET)
    LoaderConfig = mod["LoaderConfig"]
    ValidationError = mod.get("ValidationError")

    # Given: visibility_timeout이 최소 조건을 만족하지 않고
    # When: LoaderConfig를 생성하면
    # Then: ValidationError가 발생한다
    with pytest.raises(ValidationError or Exception):  # type: ignore[arg-type]
        LoaderConfig(
            queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/dev-domain-load-queue",
            query_timeout=400,
            visibility_timeout=1800,
        )

    # Given: 유효한 가시성과 백오프 값을 제공하고
    # When: LoaderConfig를 생성하면
    # Then: backoff_seconds가 설정값 그대로 유지된다
    cfg = LoaderConfig(
        queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/dev-domain-load-queue",
        query_timeout=200,
        visibility_timeout=1200,
        backoff_seconds=[3, 6],
    )
    assert list(cfg.backoff_seconds) == [3, 6]
