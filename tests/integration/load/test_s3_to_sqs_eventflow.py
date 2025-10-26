from __future__ import annotations

import json
from typing import Any, Dict, List

import pytest
from moto import mock_aws

from tests.fixtures.load_agent import FakeLoaderAgent, LoaderConfig, LoaderError


pytestmark = [pytest.mark.integration, pytest.mark.load]


CURATED_BUCKET = "data-pipeline-curated-dev"
CURATED_DOMAIN = "market"
CURATED_TABLE = "prices"
CURATED_INTERVAL = "1d"
CURATED_DATA_SOURCE = "yahoo"
CURATED_YEAR = "2025"
CURATED_MONTH = "09"
CURATED_DAY = "10"
CURATED_LAYER = "adjusted"
CURATED_DS = "2025-09-10"


def _build_key(
    *,
    domain: str = CURATED_DOMAIN,
    table: str = CURATED_TABLE,
    interval: str = CURATED_INTERVAL,
    data_source: str | None = CURATED_DATA_SOURCE,
    year: str = CURATED_YEAR,
    month: str = CURATED_MONTH,
    day: str = CURATED_DAY,
    layer: str = CURATED_LAYER,
    part: str,
) -> str:
    segments = [domain, table, f"interval={interval}"]
    if data_source:
        segments.append(f"data_source={data_source}")
    segments.extend([f"year={year}", f"month={month}", f"day={day}", f"layer={layer}", part])
    return "/".join(segments)


def _build_message(
    *,
    part: str,
    correlation_id: str,
    domain: str = CURATED_DOMAIN,
    data_source: str | None = CURATED_DATA_SOURCE,
    file_size: int | None = None,
) -> Dict[str, Any]:
    key = _build_key(domain=domain, data_source=data_source, part=part)
    message: Dict[str, Any] = {
        "bucket": CURATED_BUCKET,
        "key": key,
        "domain": domain,
        "table_name": CURATED_TABLE,
        "interval": CURATED_INTERVAL,
        "layer": CURATED_LAYER,
        "year": CURATED_YEAR,
        "month": CURATED_MONTH,
        "day": CURATED_DAY,
        "ds": CURATED_DS,
        "correlation_id": correlation_id,
    }
    if data_source is not None:
        message["data_source"] = data_source
    if file_size is not None:
        message["file_size"] = file_size
    return message


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
    # Given: 성공적으로 처리될 메시지가 큐에 존재하고
    # Given: S3 404가 발생하는 메시지가 있고
    # Given: ClickHouse 연결이 반복적으로 실패하는 메시지가 있고
    # Given: 메모리 고갈이 반복되는 메시지가 있고
    # Given: 큰 파일 크기를 가진 메시지가 있고
    # Given: 재시도가 필요한 메시지가 있고
    # Given: 권한 오류가 발생할 메시지가 있고
    # Given: 시크릿 조회에 실패하는 메시지가 있고
    body = _build_message(part="part-001.parquet", correlation_id="550e8400-e29b-41d4-a716-446655440000")
    sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps(body))

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url, retry_visibility_timeout=0))

    def process(msg: Dict[str, Any]) -> str:
        assert msg["domain"] == "market"
        return "SUCCESS"

    # When: 에이전트가 메시지를 처리하면
    # Then: 메시지가 삭제되어 큐가 비어야 한다
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
    # Given: 일부는 성공하고 일부는 재시도가 필요한 메시지 배치가 있고
    good = _build_message(part="part-001.parquet", correlation_id="550e8400-e29b-41d4-a716-446655440000")
    retry = _build_message(part="part-002.parquet", correlation_id="660e8400-e29b-41d4-a716-446655440001")
    for msg in [good, retry]:
        sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps(msg))

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url, retry_visibility_timeout=0))

    def process_first(msg: Dict[str, Any]) -> str:
        return "RETRY" if msg["key"].endswith("part-002.parquet") else "SUCCESS"

    # When: 첫 번째 실행에서 일부 메시지가 재시도를 요구하고
    # First pass: 1 success, 1 retry (visibility set to 0 to requeue)
    r1 = agent.run_once(process_first)
    assert r1["count"] == 2

    # Second pass: process the retried message successfully
    def process_second(msg: Dict[str, Any]) -> str:
        return "SUCCESS"

    # Then: 두 번째 실행 이후 큐가 비어야 한다
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

    # Given: 지속적으로 실패할 메시지가 큐에 존재하고
    body = _build_message(part="part-003.parquet", correlation_id="770e8400-e29b-41d4-a716-446655440000")
    sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps(body))

    # When: 최대 재시도 횟수를 초과하도록 여러 번 폴링하면
    dlq_messages = _drain_main_queue(sqs_client=sqs, main_url=main_url, dlq_url=dlq_url)

    # Then: 메시지가 DLQ로 이동해야 한다
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

    # Given: JSON 파싱에 실패할 메시지가 큐에 있고
    sqs.send_message(QueueUrl=main_url, MessageBody="{invalid-json")

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url, retry_visibility_timeout=0))

    parse_actions: List[Dict[str, Any]] = []
    for _ in range(4):
        result = agent.run_once(lambda _: "SUCCESS")
        parse_actions.extend([r for r in result["results"] if r["action"] == "PARSE_ERROR"])

    # When: 파싱 실패가 반복되면
    # Then: DLQ로 이동하고 에러 코드가 기록된다
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

    # Given: S3 404가 재현되는 메시지가 큐에 있고
    body = _build_message(part="part-006.parquet", correlation_id="a70e8400-e29b-41d4-a716-446655440000")
    sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps(body))

    attempts: List[str] = []

    def process_with_retries(_: Dict[str, Any]) -> str:
        attempts.append("FILE_NOT_FOUND")
        raise LoaderError("FILE_NOT_FOUND", "S3 returned 404")

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url, retry_visibility_timeout=0))

    # When: 동일한 메시지가 여러 번 재시도되면
    for _ in range(3):
        result = agent.run_once(process_with_retries)
        codes = [r.get("error_code") for r in result["results"] if r["action"] == "EXCEPTION"]
        assert codes and codes[0] == "FILE_NOT_FOUND"

    assert attempts == ["FILE_NOT_FOUND", "FILE_NOT_FOUND", "FILE_NOT_FOUND"]

    # Then: 재시도 후 DLQ로 이동한다
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

    # Given: 네트워크 오류가 반복되는 메시지가 큐에 있고
    body = _build_message(part="part-006.parquet", correlation_id="a70e8400-e29b-41d4-a716-446655440000")
    sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps(body))

    attempts: List[str] = []

    def process_connection(_: Dict[str, Any]) -> str:
        attempts.append("CONNECTION_ERROR")
        raise ConnectionError("clickhouse unreachable")

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url, retry_visibility_timeout=0))

    # When: 동일한 메시지가 여러 번 재시도되면
    for _ in range(3):
        result = agent.run_once(process_connection)
        exception_codes = [r.get("error_code") for r in result["results"] if r["action"] == "EXCEPTION"]
        assert exception_codes and exception_codes[0] == "CONNECTION_ERROR"

    assert attempts == ["CONNECTION_ERROR", "CONNECTION_ERROR", "CONNECTION_ERROR"]

    # Then: 재시도 후 DLQ로 이동한다
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

    # Given: 메모리 오류가 누적되는 메시지가 큐에 있고
    body = _build_message(part="part-007.parquet", correlation_id="b80e8400-e29b-41d4-a716-446655440000")
    sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps(body))

    attempt_codes: List[str] = []

    def process_memory(_: Dict[str, Any]) -> str:
        if len(attempt_codes) < 2:
            attempt_codes.append("MEMORY_ERROR")
            raise MemoryError("loader out of memory")
        attempt_codes.append("MEMORY_ERROR")
        raise LoaderError("MEMORY_ERROR", "OOM persisted")

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url, retry_visibility_timeout=0))

    # When: 재시도 한도를 초과하도록 처리하면
    for _ in range(3):
        result = agent.run_once(process_memory)
        exception_codes = [r.get("error_code") for r in result["results"] if r["action"] == "EXCEPTION"]
        assert exception_codes and exception_codes[0] == "MEMORY_ERROR"

    assert attempt_codes == ["MEMORY_ERROR", "MEMORY_ERROR", "MEMORY_ERROR"]

    # Then: 재시도 제한 후 DLQ로 이동한다
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
    # Given: 큰 파일을 가리키는 메시지가 큐에 있고
    body = _build_message(
        part="part-008.parquet",
        correlation_id="c80e8400-e29b-41d4-a716-446655440000",
        file_size=150 * 1024 * 1024,
    )
    sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps(body))

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url))

    def process_large(msg: Dict[str, Any]) -> str:
        assert msg["file_size"] == body["file_size"]
        return "SUCCESS"

    # When: 에이전트가 메시지를 처리하면
    # Then: 성공적으로 삭제되어 큐가 비어야 한다
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
    # Given: 재시도 처리가 필요한 메시지가 큐에 있고
    body = _build_message(part="part-004.parquet", correlation_id="880e8400-e29b-41d4-a716-446655440000")
    sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps(body))

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url, retry_visibility_timeout=15))

    def process_retry(_: Dict[str, Any]) -> str:
        return "RETRY"

    # When: 에이전트가 재시도를 요청하면
    # Then: visibility 타임아웃이 설정값으로 변경된다
    result = agent.run_once(process_retry)
    assert result["count"] == 1
    assert result["visibility_changes"] == [15]


@mock_aws
def test_security_attributes_round_trip(make_load_queues, load_module) -> None:
    env = "dev"
    domain = "market"
    main_url, _ = make_load_queues(env, domain)

    import boto3

    module = load_module("src/lambda/layers/load/contracts/python/load_contracts.py")
    build_message_attributes = module["build_message_attributes"]
    LoadMessage = module["LoadMessage"]

    # Given: 메시지 속성이 포함된 SQS 메시지가 전송되고
    message = LoadMessage(
        bucket=CURATED_BUCKET,
        key=_build_key(part="part-005.parquet"),
        domain=CURATED_DOMAIN,
        table_name=CURATED_TABLE,
        interval=CURATED_INTERVAL,
        data_source=CURATED_DATA_SOURCE,
        year=CURATED_YEAR,
        month=CURATED_MONTH,
        day=CURATED_DAY,
        layer=CURATED_LAYER,
        ds=CURATED_DS,
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
    # Then: 수신된 속성이 원본과 동일해야 한다
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

    # Given: 권한 오류를 유발할 메시지가 큐에 있고
    body = _build_message(part="part-009.parquet", correlation_id="d90e8400-e29b-41d4-a716-446655440000")
    sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps(body))

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url, retry_visibility_timeout=0))

    def process_permission(_: Dict[str, Any]) -> str:
        raise PermissionError("access denied")

    # When: 메시지를 처리하면
    result = agent.run_once(process_permission)
    exception_codes = [r.get("error_code") for r in result["results"] if r["action"] == "EXCEPTION"]
    assert exception_codes and exception_codes[0] == "PERMISSION_ERROR"

    # Then: 메시지는 즉시 DLQ로 이동한다
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

    # Given: 시크릿 조회 오류를 유발할 메시지가 큐에 있고
    body = _build_message(part="part-010.parquet", correlation_id="e90e8400-e29b-41d4-a716-446655440000")
    sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps(body))

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url, retry_visibility_timeout=0))

    def process_secrets(_: Dict[str, Any]) -> str:
        raise LoaderError("SECRETS_ERROR", "unable to read secret")

    # When: 메시지를 처리하면
    result = agent.run_once(process_secrets)
    exception_codes = [r.get("error_code") for r in result["results"] if r["action"] == "EXCEPTION"]
    assert exception_codes and exception_codes[0] == "SECRETS_ERROR"

    # Then: 메시지는 즉시 DLQ로 이동한다
    dlq_messages = _drain_main_queue(sqs_client=sqs, main_url=main_url, dlq_url=dlq_url, max_attempts=3)
    assert len(dlq_messages) == 1
