from __future__ import annotations

import json
from typing import Dict, List

import pytest
from moto import mock_aws

from tests.fixtures.load_agent import FakeLoaderAgent, LoaderConfig, LoaderError
from tests.integration.load import helpers

pytestmark = [pytest.mark.integration, pytest.mark.load]


@mock_aws
def test_partial_batch_retry_then_success(make_load_queues) -> None:
    """
    Given: 동일 파티션에서 하나는 성공, 하나는 재시도가 필요한 배치
    When: 첫 run_once에서 RETRY, 두 번째 run_once에서 SUCCESS 처리
    Then: 두 번째 실행 후 큐가 비어야 함
    """
    sqs, main_url, _ = helpers.prepare_queue(make_load_queues, visibility_timeout=0)

    success_payload = helpers.build_message(part="part-200.parquet", correlation_id="retry-partial-1")
    retry_payload = helpers.build_message(part="part-201.parquet", correlation_id="retry-partial-2")
    for payload in (success_payload, retry_payload):
        sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps(payload))

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url, retry_visibility_timeout=0))

    def process_first(msg: Dict[str, str]) -> str:
        return "RETRY" if msg["key"].endswith("part-201.parquet") else "SUCCESS"

    agent.run_once(process_first)

    def process_second(_: Dict[str, str]) -> str:
        return "SUCCESS"

    agent.run_once(process_second)

    resp = sqs.receive_message(QueueUrl=main_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
    assert resp.get("Messages", []) == []


@mock_aws
def test_parse_error_moves_to_dlq(make_load_queues) -> None:
    """
    Given: JSON 파싱이 실패하는 메시지
    When: 여러 차례 run_once 호출
    Then: PARSE_ERROR 이벤트가 기록되고 메시지는 DLQ로 이동해야 함
    """
    sqs, main_url, dlq_url = helpers.prepare_queue(make_load_queues, visibility_timeout=0)
    sqs.send_message(QueueUrl=main_url, MessageBody="{invalid-json")

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url, retry_visibility_timeout=0))
    parse_events: List[Dict[str, str]] = []
    for _ in range(4):
        result = agent.run_once(lambda _: "SUCCESS")
        parse_events.extend([entry for entry in result["results"] if entry["action"] == "PARSE_ERROR"])

    assert parse_events, "PARSE_ERROR 이벤트가 있어야 합니다"
    dlq_messages = helpers.drain_main_queue(sqs_client=sqs, main_url=main_url, dlq_url=dlq_url)
    assert len(dlq_messages) == 1


@mock_aws
def test_memory_error_escalates_to_dlq(make_load_queues) -> None:
    """
    Given: 초기에는 MemoryError, 이후 LoaderError를 던지는 프로세스
    When: 재시도 한도를 초과할 때까지 run_once 실행
    Then: MEMORY_ERROR 코드가 기록되고 DLQ로 이동해야 함
    """
    attempt_codes: List[str] = []

    def process(_: Dict[str, str]) -> str:
        if len(attempt_codes) < 2:
            attempt_codes.append("MemoryError")
            raise MemoryError("loader out of memory")
        attempt_codes.append("LoaderError")
        raise LoaderError("MEMORY_ERROR", "OOM persisted")

    dlq_messages = helpers.run_dlq_scenario(
        make_load_queues,
        part="part-202.parquet",
        process=process,
        expected_code="MEMORY_ERROR",
        attempts=3,
    )
    assert len(dlq_messages) == 1
    assert attempt_codes == ["MemoryError", "MemoryError", "LoaderError"]
