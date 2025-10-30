import json
import runpy
from typing import Any, Dict, Tuple

import boto3
import pytest

from tests.fixtures.load_builders import build_s3_object_created_event


class FakeSqsClient:
    def __init__(self) -> None:
        self.messages: list[Dict[str, Any]] = []

    def send_message(self, QueueUrl: str, MessageBody: str, MessageAttributes: Dict[str, Any]) -> Dict[str, str]:
        self.messages.append(
            {
                "QueueUrl": QueueUrl,
                "MessageBody": MessageBody,
                "MessageAttributes": MessageAttributes,
            }
        )
        return {"MessageId": "msg-123"}


def _load_module(
    monkeypatch: pytest.MonkeyPatch,
    *,
    queue_map: Dict[str, str],
    priority_map: Dict[str, str],
    min_file_size_bytes: int = 1024,
) -> Tuple[Dict[str, Any], FakeSqsClient]:
    monkeypatch.syspath_prepend("src/lambda/layers/load/contracts/python")
    monkeypatch.setenv("LOAD_QUEUE_MAP", json.dumps(queue_map))
    monkeypatch.setenv("PRIORITY_MAP", json.dumps(priority_map))
    monkeypatch.setenv("MIN_FILE_SIZE_BYTES", str(min_file_size_bytes))

    fake_sqs = FakeSqsClient()
    monkeypatch.setattr(boto3, "client", lambda service, **kwargs: fake_sqs)

    module = runpy.run_path("src/lambda/functions/load_event_publisher/handler.py")
    return module, fake_sqs


def test_main_publishes_message_for_valid_event(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Given: 도메인 큐 매핑과 유효한 S3 객체 이벤트
    When: load_event_publisher main 실행
    Then: 메시지가 큐로 전송되고 우선순위가 설정됨
    """
    queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/dev-market-load-queue"
    module, fake_sqs = _load_module(
        monkeypatch,
        queue_map={"market": queue_url},
        priority_map={"market": "1"},
    )

    event = build_s3_object_created_event(
        bucket="data-pipeline-curated-dev",
        key="market/prices/interval=1d/year=2025/month=09/day=10/layer=adjusted/file.parquet",
        size=4096,
    )

    result = module["main"](event, None)

    assert result == {"status": "SUCCESS", "queue": queue_url}
    assert len(fake_sqs.messages) == 1

    message = fake_sqs.messages[0]
    body = json.loads(message["MessageBody"])
    assert message["QueueUrl"] == queue_url
    assert body["bucket"] == event["detail"]["bucket"]["name"]
    assert body["domain"] == "market"
    assert body["table_name"] == "prices"
    assert "correlation_id" in body
    assert message["MessageAttributes"]["Priority"]["StringValue"] == "1"


def test_main_skips_when_below_size_threshold(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Given: 파일 크기가 임계값보다 작은 이벤트
    When: load_event_publisher main 실행
    Then: 상태가 SKIPPED이고 메시지 전송이 없음
    """
    module, fake_sqs = _load_module(
        monkeypatch,
        queue_map={"market": "https://example.com/queue"},
        priority_map={},
        min_file_size_bytes=4096,
    )

    event = build_s3_object_created_event(
        bucket="data-pipeline-curated-dev",
        key="market/prices/interval=1d/year=2025/month=09/day=10/layer=adjusted/file.parquet",
        size=2048,
    )

    result = module["main"](event, None)

    assert result["status"] == "SKIPPED"
    assert result["reason"] == "File size below threshold"
    assert fake_sqs.messages == []


def test_main_skips_on_validation_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Given: 포맷이 잘못된 키를 가진 이벤트
    When: load_event_publisher main 실행
    Then: 상태가 SKIPPED이고 메시지 전송이 없음
    """
    module, fake_sqs = _load_module(
        monkeypatch,
        queue_map={"market": "https://example.com/queue"},
        priority_map={},
    )

    # Invalid key (missing required partitions and parquet suffix)
    event = build_s3_object_created_event(
        bucket="data-pipeline-curated-dev",
        key="market/prices/invalid.txt",
        size=4096,
    )

    result = module["main"](event, None)

    assert result["status"] == "SKIPPED"
    assert "Invalid curated key format" in result["reason"] or result["reason"]
    assert fake_sqs.messages == []
