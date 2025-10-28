import pytest
from typing import Any, Dict


pytestmark = [pytest.mark.unit, pytest.mark.load]


TARGET = "src/lambda/layers/load/contracts/python/load_contracts.py"


def test_loader_config_defaults(load_module) -> None:
    """
    Given: 최소 필수 입력만 제공
    When: LoaderConfig를 생성
    Then: 모든 기본값이 스펙과 일치
    """
    mod: Dict[str, Any] = load_module(TARGET)
    LoaderConfig = mod["LoaderConfig"]

    cfg = LoaderConfig(queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/dev-domain-load-queue")
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
    """
    Given: 특정 필드가 허용 범위를 벗어남
    When: LoaderConfig를 생성
    Then: ValidationError 발생
    """
    mod: Dict[str, Any] = load_module(TARGET)
    LoaderConfig = mod["LoaderConfig"]
    ValidationError = mod.get("ValidationError")

    base = {"queue_url": "https://sqs.us-east-1.amazonaws.com/123456789012/dev-domain-load-queue"}
    base.update(kwargs)

    with pytest.raises(ValidationError or Exception):  # type: ignore[arg-type]
        LoaderConfig(**base)


def test_loader_config_visibility_multiple(load_module) -> None:
    """
    Given: visibility_timeout과 query_timeout 조합이 스펙을 위반
    When: LoaderConfig를 생성
    Then: ValidationError 발생 후 유효한 값은 그대로 적용
    """
    mod: Dict[str, Any] = load_module(TARGET)
    LoaderConfig = mod["LoaderConfig"]
    ValidationError = mod.get("ValidationError")

    with pytest.raises(ValidationError or Exception):  # type: ignore[arg-type]
        LoaderConfig(
            queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/dev-domain-load-queue",
            query_timeout=400,
            visibility_timeout=1800,
        )

    cfg = LoaderConfig(
        queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/dev-domain-load-queue",
        query_timeout=200,
        visibility_timeout=1200,
        backoff_seconds=[3, 6],
    )
    assert list(cfg.backoff_seconds) == [3, 6]
