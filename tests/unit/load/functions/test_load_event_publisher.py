import json
import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pytest

from tests.fixtures.load_builders import build_s3_object_created_event

TARGET = "src/lambda/functions/load_event_publisher/handler.py"
CURATED_BUCKET = "data-pipeline-curated-dev"
CURATED_KEY = "market/prices/interval=1d/data_source=yahoo/year=2025/month=09/day=10/layer=adjusted/part-0000.parquet"
INDICATOR_KEY = (
    "market/prices/interval=1d/data_source=yahoo/year=2025/month=09/day=10/layer=technical_indicator/part-0000.parquet"
)
COMPACTED_KEY = (
    "market/prices/interval=1d/data_source=yahoo/year=2025/month=09/day=10/layer=compacted/part-0000.parquet"
)


class FakeSQSClient:
    def __init__(self) -> None:
        self.calls: list[Dict[str, Any]] = []

    def send_message(self, *, QueueUrl: str, MessageBody: str, MessageAttributes: Dict[str, Any]) -> None:
        self.calls.append({"QueueUrl": QueueUrl, "MessageBody": MessageBody, "MessageAttributes": MessageAttributes})


@pytest.fixture
def load_publisher(monkeypatch: pytest.MonkeyPatch, load_module) -> Callable[..., Tuple[Dict[str, Any], FakeSQSClient]]:
    def _load(
        *,
        queue_map: Dict[str, str] | None = None,
        priority_map: Dict[str, str] | None = None,
        min_file_size: str = "1024",
        allowed_layers: Optional[List[str]] = None,
    ) -> Tuple[Dict[str, Any], FakeSQSClient]:
        queue_map = queue_map or {
            "market": "https://sqs.ap-northeast-2.amazonaws.com/123456789012/dev-market-load-queue"
        }
        priority_map = priority_map or {"market": "1"}
        allowed_layers = allowed_layers or ["adjusted", "technical_indicator"]

        monkeypatch.setenv("LOAD_QUEUE_MAP", json.dumps(queue_map))
        monkeypatch.setenv("PRIORITY_MAP", json.dumps(priority_map))
        monkeypatch.setenv("MIN_FILE_SIZE_BYTES", min_file_size)
        monkeypatch.setenv("ALLOWED_LAYERS", json.dumps(allowed_layers))
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test-access")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
        monkeypatch.setenv("AWS_SESSION_TOKEN", "test-token")

        fake_sqs = FakeSQSClient()

        def _fake_boto_client(service_name: str, *args: Any, **kwargs: Any) -> FakeSQSClient:
            assert service_name == "sqs"
            return fake_sqs

        monkeypatch.setattr("boto3.client", _fake_boto_client, raising=True)

        layer_dir = Path(__file__).resolve().parents[4] / "src" / "lambda" / "shared" / "layers" / "core" / "python"
        monkeypatch.syspath_prepend(str(layer_dir))

        module_globals = load_module(TARGET)
        module_globals["_sqs"] = fake_sqs
        module_globals["LOGGER"] = logging.getLogger("test")
        return module_globals, fake_sqs

    return _load


def test_main_publishes_message(load_publisher) -> None:
    # Given: 유효한 S3 Object Created 이벤트와 큐 매핑이 준비되고
    module, fake_sqs = load_publisher()
    event = build_s3_object_created_event(bucket=CURATED_BUCKET, key=CURATED_KEY, size=4096)

    # When: Load Event Publisher를 실행하면
    result = module["main"](event, None)

    # Then: 메시지가 정상적으로 발행되어 SQS에 기록된다
    assert result == {
        "status": "SUCCESS",
        "queue": "https://sqs.ap-northeast-2.amazonaws.com/123456789012/dev-market-load-queue",
    }
    assert len(fake_sqs.calls) == 1

    payload = json.loads(fake_sqs.calls[0]["MessageBody"])
    assert payload["domain"] == "market"
    assert payload["table_name"] == "prices"
    assert payload["layer"] == "adjusted"
    assert payload["ds"] == "2025-09-10"
    assert "correlation_id" in payload and payload["correlation_id"]


def test_main_skips_small_file(load_publisher) -> None:
    # Given: 최소 파일 크기보다 작은 이벤트가 주어지고
    module, fake_sqs = load_publisher(min_file_size="2048")
    small_event = build_s3_object_created_event(bucket=CURATED_BUCKET, key=CURATED_KEY, size=1024)

    # When: Load Event Publisher를 실행하면
    result = module["main"](small_event, None)

    # Then: 메시지는 발행되지 않고 SKIPPED 상태가 반환된다
    assert result == {"status": "SKIPPED", "reason": "File size below threshold"}
    assert fake_sqs.calls == []


def test_main_publishes_indicator_layer(load_publisher) -> None:
    module, fake_sqs = load_publisher()
    event = build_s3_object_created_event(bucket=CURATED_BUCKET, key=INDICATOR_KEY, size=4096)

    result = module["main"](event, None)

    assert result["status"] == "SUCCESS"
    payload = json.loads(fake_sqs.calls[0]["MessageBody"])
    assert payload["layer"] == "technical_indicator"


def test_main_skips_unknown_domain(load_publisher) -> None:
    # Given: 큐 매핑에 존재하지 않는 도메인의 이벤트가 주어지고
    module, fake_sqs = load_publisher()
    unknown_key = (
        "unknown/prices/interval=1d/data_source=yahoo/year=2025/month=09/day=10/layer=adjusted/part-0000.parquet"
    )
    event = build_s3_object_created_event(bucket=CURATED_BUCKET, key=unknown_key, size=4096)

    # When: Load Event Publisher를 실행하면
    result = module["main"](event, None)

    # Then: 메시지가 드롭되고 Unknown domain 사유로 SKIPPED가 반환된다
    assert result == {"status": "SKIPPED", "reason": "Unknown domain"}
    assert fake_sqs.calls == []


def test_main_skips_on_validation_error(load_publisher) -> None:
    # Given: 스키마 검증에 실패할 잘못된 확장자의 이벤트가 주어지고
    module, fake_sqs = load_publisher()
    invalid_key = "market/prices/interval=1d/data_source=yahoo/year=2025/month=09/day=10/layer=adjusted/part-0000.csv"
    event = build_s3_object_created_event(bucket=CURATED_BUCKET, key=invalid_key, size=4096)

    # When: Load Event Publisher를 실행하면
    result = module["main"](event, None)

    # Then: ValidationError로 인해 이벤트가 무시되고 큐 전송은 수행되지 않는다
    assert result["status"] == "SKIPPED"
    assert "Parquet" in result["reason"]
    assert fake_sqs.calls == []


def test_main_skips_disallowed_layer(load_publisher) -> None:
    module, fake_sqs = load_publisher(allowed_layers=["adjusted", "technical_indicator"])
    event = build_s3_object_created_event(bucket=CURATED_BUCKET, key=COMPACTED_KEY, size=4096)

    result = module["main"](event, None)

    assert result["status"] == "SKIPPED"
    assert "Layer not allowed" in result["reason"]
    assert fake_sqs.calls == []
