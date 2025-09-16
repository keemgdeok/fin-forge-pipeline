from __future__ import annotations

import json
from typing import Any, Dict, List

import pytest
from moto import mock_aws

from tests.fixtures.load_agent import FakeLoaderAgent, LoaderConfig


pytestmark = [pytest.mark.integration, pytest.mark.load]


def _drain_main_queue(
    *,
    sqs_client,
    main_url: str,
    dlq_url: str,
    max_attempts: int = 4,
    visibility_timeout: int = 0,
) -> List[Dict[str, Any]]:
    """Receive a message up to ``max_attempts`` times to trigger DLQ redrive.

    Moto의 SQS 구현은 ``ReceiveMessage``가 호출될 때마다 ApproximateReceiveCount를 증가시키므로
    maxReceiveCount + 1 번 폴링하면 메시지가 DLQ로 이동한다. 이 헬퍼는 테스트에서 해당 시퀀스를
    반복 구현하지 않도록 캡슐화한다.
    """

    for _ in range(max_attempts):
        resp = sqs_client.receive_message(
            QueueUrl=main_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=1,
            AttributeNames=["ApproximateReceiveCount"],
        )
        messages = resp.get("Messages", [])
        if not messages:
            break
        sqs_client.change_message_visibility(
            QueueUrl=main_url,
            ReceiptHandle=messages[0]["ReceiptHandle"],
            VisibilityTimeout=visibility_timeout,
        )

    dlq_resp = sqs_client.receive_message(QueueUrl=dlq_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    return dlq_resp.get("Messages", [])


@mock_aws
def test_happy_path_ack_deletes_message(make_load_queues) -> None:
    env = "dev"
    domain = "market"
    main_url, _ = make_load_queues(env, domain)

    import boto3

    sqs = boto3.client("sqs", region_name="us-east-1")
    body = {
        "bucket": "data-pipeline-curated-dev",
        "key": "market/prices/ds=2025-09-10/part-001.parquet",
        "domain": "market",
        "table_name": "prices",
        "partition": "ds=2025-09-10",
        "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
    }
    sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps(body))

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url, retry_visibility_timeout=0))

    def process(msg: Dict[str, Any]) -> str:
        assert msg["domain"] == "market"
        return "SUCCESS"

    result = agent.run_once(process)
    assert result["count"] == 1

    # Queue should be empty after ACK
    resp = sqs.receive_message(QueueUrl=main_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
    assert resp.get("Messages", []) == []


@mock_aws
def test_partial_batch_retry_then_success(make_load_queues) -> None:
    env = "dev"
    domain = "market"
    main_url, _ = make_load_queues(env, domain)

    import boto3

    sqs = boto3.client("sqs", region_name="us-east-1")
    good = {
        "bucket": "data-pipeline-curated-dev",
        "key": "market/prices/ds=2025-09-10/part-001.parquet",
        "domain": "market",
        "table_name": "prices",
        "partition": "ds=2025-09-10",
        "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
    }
    retry = {
        "bucket": "data-pipeline-curated-dev",
        "key": "market/prices/ds=2025-09-10/part-002.parquet",
        "domain": "market",
        "table_name": "prices",
        "partition": "ds=2025-09-10",
        "correlation_id": "660e8400-e29b-41d4-a716-446655440001",
    }
    for msg in [good, retry]:
        sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps(msg))

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url, retry_visibility_timeout=0))

    def process_first(msg: Dict[str, Any]) -> str:
        return "RETRY" if msg["key"].endswith("part-002.parquet") else "SUCCESS"

    # First pass: 1 success, 1 retry (visibility set to 0 to requeue)
    r1 = agent.run_once(process_first)
    assert r1["count"] == 2

    # Second pass: process the retried message successfully
    def process_second(msg: Dict[str, Any]) -> str:
        return "SUCCESS"

    r2 = agent.run_once(process_second)
    assert r2["count"] >= 1  # at least the retried message

    # Queue should be empty after second pass
    resp = sqs.receive_message(QueueUrl=main_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
    assert resp.get("Messages", []) == []


@mock_aws
def test_dlq_after_max_receive_count(make_load_queues) -> None:
    env = "dev"
    domain = "market"
    main_url, dlq_url = make_load_queues(env, domain)

    import boto3

    sqs = boto3.client("sqs", region_name="us-east-1")
    # Reduce visibility timeout for the test so we can reprocess quickly
    sqs.set_queue_attributes(QueueUrl=main_url, Attributes={"VisibilityTimeout": "0"})

    body = {
        "bucket": "data-pipeline-curated-dev",
        "key": "market/prices/ds=2025-09-10/part-003.parquet",
        "domain": "market",
        "table_name": "prices",
        "partition": "ds=2025-09-10",
        "correlation_id": "770e8400-e29b-41d4-a716-446655440000",
    }
    sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps(body))

    dlq_messages = _drain_main_queue(sqs_client=sqs, main_url=main_url, dlq_url=dlq_url)
    assert len(dlq_messages) == 1
    dlq_body = json.loads(dlq_messages[0]["Body"])
    assert dlq_body["key"].endswith("part-003.parquet")


@mock_aws
def test_parse_error_moves_to_dlq(make_load_queues) -> None:
    env = "dev"
    domain = "market"
    main_url, dlq_url = make_load_queues(env, domain)

    import boto3

    sqs = boto3.client("sqs", region_name="us-east-1")
    sqs.set_queue_attributes(QueueUrl=main_url, Attributes={"VisibilityTimeout": "0"})

    sqs.send_message(QueueUrl=main_url, MessageBody="{invalid-json")

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url, retry_visibility_timeout=0))

    parse_actions: List[Dict[str, Any]] = []
    for _ in range(4):
        result = agent.run_once(lambda _: "SUCCESS")
        parse_actions.extend([r for r in result["results"] if r["action"] == "PARSE_ERROR"])

    assert parse_actions, "parse failures should be recorded"

    dlq_messages = _drain_main_queue(sqs_client=sqs, main_url=main_url, dlq_url=dlq_url)
    assert len(dlq_messages) == 1


@mock_aws
def test_connection_error_retries_then_dlq(make_load_queues) -> None:
    env = "dev"
    domain = "market"
    main_url, dlq_url = make_load_queues(env, domain)

    import boto3

    sqs = boto3.client("sqs", region_name="us-east-1")
    sqs.set_queue_attributes(QueueUrl=main_url, Attributes={"VisibilityTimeout": "0"})

    body = {
        "bucket": "data-pipeline-curated-dev",
        "key": "market/prices/ds=2025-09-10/part-006.parquet",
        "domain": "market",
        "table_name": "prices",
        "partition": "ds=2025-09-10",
        "correlation_id": "a70e8400-e29b-41d4-a716-446655440000",
    }
    sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps(body))

    attempts = {"count": 0}

    def process_with_retries(msg: Dict[str, Any]) -> str:
        attempts["count"] += 1
        if attempts["count"] <= 3:
            return "RETRY"  # connection issue recovered with backoff
        return "FAIL"  # after retries exhausted

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url, retry_visibility_timeout=0))

    for _ in range(4):
        agent.run_once(process_with_retries)

    dlq_messages = _drain_main_queue(sqs_client=sqs, main_url=main_url, dlq_url=dlq_url)
    assert len(dlq_messages) == 1
    dlq_body = json.loads(dlq_messages[0]["Body"])
    assert dlq_body["correlation_id"] == body["correlation_id"]


@mock_aws
def test_large_file_processing_success(make_load_queues) -> None:
    env = "dev"
    domain = "market"
    main_url, _ = make_load_queues(env, domain)

    import boto3

    sqs = boto3.client("sqs", region_name="us-east-1")
    body = {
        "bucket": "data-pipeline-curated-dev",
        "key": "market/prices/ds=2025-09-10/part-007.parquet",
        "domain": "market",
        "table_name": "prices",
        "partition": "ds=2025-09-10",
        "correlation_id": "b80e8400-e29b-41d4-a716-446655440000",
        "file_size": 150 * 1024 * 1024,
    }
    sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps(body))

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url))

    def process_large(msg: Dict[str, Any]) -> str:
        assert msg["file_size"] == body["file_size"]
        return "SUCCESS"

    result = agent.run_once(process_large)
    assert result["count"] == 1

    resp = sqs.receive_message(QueueUrl=main_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
    assert resp.get("Messages", []) == []


@mock_aws
def test_retry_visibility_extension(make_load_queues) -> None:
    env = "dev"
    domain = "market"
    main_url, _ = make_load_queues(env, domain)

    import boto3

    sqs = boto3.client("sqs", region_name="us-east-1")
    body = {
        "bucket": "data-pipeline-curated-dev",
        "key": "market/prices/ds=2025-09-10/part-004.parquet",
        "domain": "market",
        "table_name": "prices",
        "partition": "ds=2025-09-10",
        "correlation_id": "880e8400-e29b-41d4-a716-446655440000",
    }
    sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps(body))

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url, retry_visibility_timeout=15))

    def process_retry(_: Dict[str, Any]) -> str:
        return "RETRY"

    result = agent.run_once(process_retry)
    assert result["count"] == 1
    assert result["visibility_changes"] == [15]


@mock_aws
def test_security_attributes_round_trip(make_load_queues, load_module) -> None:
    env = "dev"
    domain = "market"
    main_url, _ = make_load_queues(env, domain)

    import boto3

    module = load_module("src/lambda/shared/layers/core/load_contracts.py")
    build_message_attributes = module["build_message_attributes"]
    LoadMessage = module["LoadMessage"]

    message = LoadMessage(
        bucket="data-pipeline-curated-dev",
        key="market/prices/ds=2025-09-10/part-005.parquet",
        domain="market",
        table_name="prices",
        partition="ds=2025-09-10",
        correlation_id="990e8400-e29b-41d4-a716-446655440000",
    )
    attrs = build_message_attributes(message)

    sqs = boto3.client("sqs", region_name="us-east-1")
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
    msg = resp["Messages"][0]
    received_attrs = msg["MessageAttributes"]
    assert received_attrs["Domain"]["StringValue"] == "market"
    assert received_attrs["Priority"]["StringValue"] == "1"
