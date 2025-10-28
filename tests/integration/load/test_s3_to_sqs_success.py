from __future__ import annotations

import json
from typing import Dict

import pytest
from moto import mock_aws

from tests.fixtures.load_agent import FakeLoaderAgent, LoaderConfig
from tests.integration.load import helpers

pytestmark = [pytest.mark.integration, pytest.mark.load]


@mock_aws
def test_success_message_is_deleted(make_load_queues) -> None:
    """
    Given: 로더 큐에 정상 메시지가 존재
    When: FakeLoaderAgent가 메시지를 성공 처리
    Then: 메시지가 삭제되어 큐가 비어야 함
    """
    sqs, main_url, _ = helpers.prepare_queue(make_load_queues)
    sqs.send_message(
        QueueUrl=main_url,
        MessageBody=json.dumps(helpers.build_message(part="part-001.parquet", correlation_id="success-0001")),
    )

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url, retry_visibility_timeout=0))

    def process(msg: Dict[str, str]) -> str:
        assert msg["domain"] == helpers.CURATED_DOMAIN
        return "SUCCESS"

    result = agent.run_once(process)
    assert result["count"] == 1
    resp = sqs.receive_message(QueueUrl=main_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
    assert resp.get("Messages", []) == []


@mock_aws
def test_fail_action_leaves_message_in_queue(make_load_queues) -> None:
    """
    Given: 로더가 FAIL을 반환하는 메시지
    When: 한 번 처리 시도
    Then: 메시지는 삭제되지 않고 큐에 남아야 함
    """
    sqs, main_url, _ = helpers.prepare_queue(make_load_queues, visibility_timeout=0)
    sqs.send_message(
        QueueUrl=main_url,
        MessageBody=json.dumps(helpers.build_message(part="part-002.parquet", correlation_id="fail-0001")),
    )

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url, retry_visibility_timeout=0))

    def process(_: Dict[str, str]) -> str:
        return "FAIL"

    result = agent.run_once(process)
    assert result["count"] == 1

    resp = sqs.receive_message(QueueUrl=main_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert resp.get("Messages"), "FAIL 후에도 메시지가 큐에 남아 있어야 합니다"


@mock_aws
def test_large_file_processing_success(make_load_queues) -> None:
    """
    Given: 파일 크기 정보가 포함된 메시지
    When: 로더가 SUCCESS를 반환
    Then: 파일 크기 값을 유지한 채 메시지가 삭제되어야 함
    """
    sqs, main_url, _ = helpers.prepare_queue(make_load_queues)
    payload = helpers.build_message(
        part="part-102.parquet",
        correlation_id="large-file-0001",
        file_size=150 * 1024 * 1024,
    )
    sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps(payload))

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url))

    def process(message: Dict[str, int]) -> str:
        assert message["file_size"] == payload["file_size"]
        return "SUCCESS"

    agent.run_once(process)
    resp = sqs.receive_message(QueueUrl=main_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
    assert resp.get("Messages", []) == []


@mock_aws
def test_retry_visibility_extension(make_load_queues) -> None:
    """
    Given: RETRY가 필요한 메시지
    When: 로더가 RETRY를 반환
    Then: ChangeMessageVisibility에 설정된 재시도 시간이 반영되어야 함
    """
    sqs, main_url, _ = helpers.prepare_queue(make_load_queues)
    sqs.send_message(
        QueueUrl=main_url,
        MessageBody=json.dumps(helpers.build_message(part="part-103.parquet", correlation_id="retry-visibility-0001")),
    )

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url, retry_visibility_timeout=15))

    def process(_: Dict[str, str]) -> str:
        return "RETRY"

    result = agent.run_once(process)
    assert result["visibility_changes"] == [15]


@mock_aws
def test_security_attributes_round_trip(make_load_queues, load_module) -> None:
    """
    Given: LoadMessage 기반 SQS 메시지 속성
    When: 메시지를 수신하여 속성을 확인
    Then: Domain과 Priority 값이 원본과 동일해야 함
    """
    sqs, main_url, _ = helpers.prepare_queue(make_load_queues)

    module = load_module("src/lambda/layers/load/contracts/python/load_contracts.py")
    build_message_attributes = module["build_message_attributes"]
    LoadMessage = module["LoadMessage"]

    message = LoadMessage(
        bucket=helpers.CURATED_BUCKET,
        key=helpers.build_key(part="part-104.parquet"),
        domain=helpers.CURATED_DOMAIN,
        table_name=helpers.CURATED_TABLE,
        interval=helpers.CURATED_INTERVAL,
        data_source=helpers.CURATED_DATA_SOURCE,
        year=helpers.CURATED_YEAR,
        month=helpers.CURATED_MONTH,
        day=helpers.CURATED_DAY,
        layer=helpers.CURATED_LAYER,
        ds=helpers.CURATED_DS,
        correlation_id="550e8400-e29b-41d4-a716-446655440000",
    )
    attrs = build_message_attributes(message)

    sqs.send_message(
        QueueUrl=main_url,
        MessageBody=json.dumps(message.to_dict()),
        MessageAttributes={k: {"StringValue": v, "DataType": "String"} for k, v in attrs.items()},
    )

    resp = sqs.receive_message(
        QueueUrl=main_url,
        MaxNumberOfMessages=1,
        MessageAttributeNames=["All"],
        WaitTimeSeconds=1,
    )
    received = resp["Messages"][0]["MessageAttributes"]
    assert received["Domain"]["StringValue"] == "market"
    assert received["Priority"]["StringValue"] == "1"
