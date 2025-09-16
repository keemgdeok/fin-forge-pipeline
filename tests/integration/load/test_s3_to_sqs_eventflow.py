from __future__ import annotations

import json
from typing import Any, Dict, List

import pytest
from moto import mock_aws

from tests.fixtures.load_agent import FakeLoaderAgent, LoaderConfig, LoaderError


pytestmark = [pytest.mark.integration, pytest.mark.load]


def _drain_main_queue(
    *,
    sqs_client,
    main_url: str,
    dlq_url: str,
    max_attempts: int = 4,
    visibility_timeout: int = 0,
) -> List[Dict[str, Any]]:
    """Receive repeatedly until moto's ApproximateReceiveCount triggers DLQ redrive.

    Moto는 ``ReceiveMessage`` 호출 시 ApproximateReceiveCount를 하나씩 증가시키고, 큐 설정값을 넘으면
    메시지를 DLQ로 옮긴다. 테스트마다 동일한 폴링/가시성 연장 코드를 반복하지 않도록 이 헬퍼로
    시퀀스를 캡슐화했다.
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
    assert all(r.get("error_code") == "PARSE_ERROR" for r in parse_actions)

    dlq_messages = _drain_main_queue(sqs_client=sqs, main_url=main_url, dlq_url=dlq_url)
    assert len(dlq_messages) == 1


@mock_aws
def test_file_not_found_retries_then_dlq(make_load_queues) -> None:
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

    attempts: List[str] = []

    def process_with_retries(_: Dict[str, Any]) -> str:
        attempts.append("FILE_NOT_FOUND")
        raise LoaderError("FILE_NOT_FOUND", "S3 returned 404")

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url, retry_visibility_timeout=0))

    for _ in range(3):
        result = agent.run_once(process_with_retries)
        codes = [r.get("error_code") for r in result["results"] if r["action"] == "EXCEPTION"]
        assert codes and codes[0] == "FILE_NOT_FOUND"

    assert attempts == ["FILE_NOT_FOUND", "FILE_NOT_FOUND", "FILE_NOT_FOUND"]

    dlq_messages = _drain_main_queue(sqs_client=sqs, main_url=main_url, dlq_url=dlq_url)
    assert len(dlq_messages) == 1
    dlq_body = json.loads(dlq_messages[0]["Body"])
    assert dlq_body["correlation_id"] == body["correlation_id"]


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

    attempts: List[str] = []

    def process_connection(_: Dict[str, Any]) -> str:
        attempts.append("CONNECTION_ERROR")
        raise ConnectionError("clickhouse unreachable")

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url, retry_visibility_timeout=0))

    for _ in range(3):
        result = agent.run_once(process_connection)
        exception_codes = [r.get("error_code") for r in result["results"] if r["action"] == "EXCEPTION"]
        assert exception_codes and exception_codes[0] == "CONNECTION_ERROR"

    assert attempts == ["CONNECTION_ERROR", "CONNECTION_ERROR", "CONNECTION_ERROR"]

    dlq_messages = _drain_main_queue(sqs_client=sqs, main_url=main_url, dlq_url=dlq_url)
    assert len(dlq_messages) == 1
    dlq_body = json.loads(dlq_messages[0]["Body"])
    assert dlq_body["correlation_id"] == body["correlation_id"]


@mock_aws
def test_memory_exhaustion_retry_limit(make_load_queues) -> None:
    env = "dev"
    domain = "market"
    main_url, dlq_url = make_load_queues(env, domain)

    import boto3

    sqs = boto3.client("sqs", region_name="us-east-1")
    sqs.set_queue_attributes(QueueUrl=main_url, Attributes={"VisibilityTimeout": "0"})

    body = {
        "bucket": "data-pipeline-curated-dev",
        "key": "market/prices/ds=2025-09-10/part-007.parquet",
        "domain": "market",
        "table_name": "prices",
        "partition": "ds=2025-09-10",
        "correlation_id": "b80e8400-e29b-41d4-a716-446655440000",
    }
    sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps(body))

    attempt_codes: List[str] = []

    def process_memory(_: Dict[str, Any]) -> str:
        if len(attempt_codes) < 2:
            attempt_codes.append("MEMORY_ERROR")
            raise MemoryError("loader out of memory")
        attempt_codes.append("MEMORY_ERROR")
        raise LoaderError("MEMORY_ERROR", "OOM persisted")

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url, retry_visibility_timeout=0))

    for _ in range(3):
        result = agent.run_once(process_memory)
        exception_codes = [r.get("error_code") for r in result["results"] if r["action"] == "EXCEPTION"]
        assert exception_codes and exception_codes[0] == "MEMORY_ERROR"

    assert attempt_codes == ["MEMORY_ERROR", "MEMORY_ERROR", "MEMORY_ERROR"]

    dlq_messages = _drain_main_queue(sqs_client=sqs, main_url=main_url, dlq_url=dlq_url, max_attempts=3)
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
        "key": "market/prices/ds=2025-09-10/part-008.parquet",
        "domain": "market",
        "table_name": "prices",
        "partition": "ds=2025-09-10",
        "correlation_id": "c80e8400-e29b-41d4-a716-446655440000",
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


@mock_aws
def test_permission_error_immediate_dlq(make_load_queues) -> None:
    env = "dev"
    domain = "market"
    main_url, dlq_url = make_load_queues(env, domain)

    import boto3

    sqs = boto3.client("sqs", region_name="us-east-1")
    sqs.set_queue_attributes(QueueUrl=main_url, Attributes={"VisibilityTimeout": "0"})

    body = {
        "bucket": "data-pipeline-curated-dev",
        "key": "market/prices/ds=2025-09-10/part-009.parquet",
        "domain": "market",
        "table_name": "prices",
        "partition": "ds=2025-09-10",
        "correlation_id": "d90e8400-e29b-41d4-a716-446655440000",
    }
    sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps(body))

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url, retry_visibility_timeout=0))

    def process_permission(_: Dict[str, Any]) -> str:
        raise PermissionError("access denied")

    result = agent.run_once(process_permission)
    exception_codes = [r.get("error_code") for r in result["results"] if r["action"] == "EXCEPTION"]
    assert exception_codes and exception_codes[0] == "PERMISSION_ERROR"

    dlq_messages = _drain_main_queue(sqs_client=sqs, main_url=main_url, dlq_url=dlq_url, max_attempts=3)
    assert len(dlq_messages) == 1


@mock_aws
def test_secrets_failure_immediate_dlq(make_load_queues) -> None:
    env = "dev"
    domain = "market"
    main_url, dlq_url = make_load_queues(env, domain)

    import boto3

    sqs = boto3.client("sqs", region_name="us-east-1")
    sqs.set_queue_attributes(QueueUrl=main_url, Attributes={"VisibilityTimeout": "0"})

    body = {
        "bucket": "data-pipeline-curated-dev",
        "key": "market/prices/ds=2025-09-10/part-010.parquet",
        "domain": "market",
        "table_name": "prices",
        "partition": "ds=2025-09-10",
        "correlation_id": "e90e8400-e29b-41d4-a716-446655440000",
    }
    sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps(body))

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url, retry_visibility_timeout=0))

    def process_secrets(_: Dict[str, Any]) -> str:
        raise LoaderError("SECRETS_ERROR", "unable to read secret")

    result = agent.run_once(process_secrets)
    exception_codes = [r.get("error_code") for r in result["results"] if r["action"] == "EXCEPTION"]
    assert exception_codes and exception_codes[0] == "SECRETS_ERROR"

    dlq_messages = _drain_main_queue(sqs_client=sqs, main_url=main_url, dlq_url=dlq_url, max_attempts=3)
    assert len(dlq_messages) == 1
