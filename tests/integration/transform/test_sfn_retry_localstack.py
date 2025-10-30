"""Step Functions 동작을 LocalStack에서 재현하는 통합 테스트입니다.

Glue 작업을 모사하는 스텁 Lambda가 연결된 최소 상태 머신을 배포하여,
동시성 스로틀링과 다중 manifest Map 실행을 재현하고 실제 구성과 동일한
재시도·순차 처리 동작을 확인합니다.

전제 조건:
    - StepFunctions와 Lambda 서비스가 활성화된 LocalStack이 실행 중이어야 합니다.
      기본 엔드포인트는 `http://localhost:4566`이며 `LOCALSTACK_ENDPOINT`로 변경 가능합니다.
    - 테스트는 가짜 자격 증명을 사용하므로 실제 AWS 자격 증명은 필요하지 않습니다.
"""

from __future__ import annotations

import io
import json
import os
import time
import uuid
import zipfile
from dataclasses import dataclass
import contextlib
from typing import Dict, Iterable, List

import boto3
import pytest
from botocore.exceptions import BotoCoreError, ClientError, EndpointConnectionError

pytestmark = pytest.mark.slow

# LocalStack 접속 기본값(환경 변수로 재정의 가능)
LOCALSTACK_ENDPOINT = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566")
LOCALSTACK_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
LOCALSTACK_ACCOUNT_ID = os.environ.get("LOCALSTACK_ACCOUNT_ID", "000000000000")


def _client(service: str) -> boto3.client:
    """LocalStack 엔드포인트를 가리키는 boto3 클라이언트를 생성합니다."""
    return boto3.client(
        service,
        region_name=LOCALSTACK_REGION,
        endpoint_url=LOCALSTACK_ENDPOINT,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    )


def _ensure_localstack() -> None:
    """LocalStack 연결을 확인하고 사용할 수 없으면 테스트를 건너뜁니다."""
    try:
        _client("stepfunctions").list_state_machines()
    except EndpointConnectionError:
        pytest.skip("LocalStack endpoint not reachable; skipping Step Functions retry tests")
    except ClientError:
        # LocalStack이 비어 있을 때 4xx를 반환하더라도 정상 상황으로 간주한다.
        pass
    except BotoCoreError as exc:
        pytest.skip(f"LocalStack not ready ({exc}); skipping Step Functions retry tests")


def _zip_lambda(code: str) -> bytes:
    """제공된 코드를 메모리 기반 zip 파일로 패키징합니다."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        info = zipfile.ZipInfo("index.py")
        info.external_attr = 0o755 << 16  # 컨테이너 내에서 실행 권한을 유지하도록 설정
        zf.writestr(info, code)
    buffer.seek(0)
    return buffer.read()


def _create_retry_stub_lambda(function_name: str) -> str:
    """실패 횟수만큼 예외를 발생시키는 스텁 Lambda를 생성합니다."""
    lambda_client = _client("lambda")
    code = """
import json
import os

class GlueConcurrentRunsExceededException(Exception):
    \"\"\"Raised to emulate Glue concurrency throttling.\"\"\"


def lambda_handler(event, _context):
    attempt_id = event["attempt_id"]
    max_failures = int(event["max_failures"])
    attempts = ATTEMPT_COUNTS.setdefault(attempt_id, 0) + 1
    ATTEMPT_COUNTS[attempt_id] = attempts

    if attempts <= max_failures:
        raise GlueConcurrentRunsExceededException(f\"Simulated throttling (attempt {attempts})\")

    return {
        "status": "SUCCEEDED",
        "attempts": attempts,
    }
"""
    # 전역 딕셔너리 정의를 코드에 앞부분에 배치
    code = "ATTEMPT_COUNTS = {}\n\n" + code
    lambda_client.create_function(
        FunctionName=function_name,
        Runtime="python3.11",
        Role=f"arn:aws:iam::{LOCALSTACK_ACCOUNT_ID}:role/dummy-role",
        Handler="index.lambda_handler",
        Code={"ZipFile": _zip_lambda(code)},
        Timeout=10,
    )
    return f"arn:aws:lambda:{LOCALSTACK_REGION}:{LOCALSTACK_ACCOUNT_ID}:function:{function_name}"


def _create_manifest_stub_lambda(function_name: str) -> str:
    """manifest 호출 횟수를 기록하는 스텁 Lambda를 생성합니다."""
    lambda_client = _client("lambda")
    code = """
import json

MANIFEST_CALLS = {}


def lambda_handler(event, _context):
    manifest_key = event["manifest_key"]
    count = MANIFEST_CALLS.setdefault(manifest_key, 0) + 1
    MANIFEST_CALLS[manifest_key] = count
    return {
        "manifest_key": manifest_key,
        "calls": count,
    }
"""
    lambda_client.create_function(
        FunctionName=function_name,
        Runtime="python3.11",
        Role=f"arn:aws:iam::{LOCALSTACK_ACCOUNT_ID}:role/dummy-role",
        Handler="index.lambda_handler",
        Code={"ZipFile": _zip_lambda(code)},
        Timeout=10,
    )
    return f"arn:aws:lambda:{LOCALSTACK_REGION}:{LOCALSTACK_ACCOUNT_ID}:function:{function_name}"


def _wait_for_lambda_active(function_name: str, timeout_seconds: int = 120) -> None:
    """Lambda가 Active 상태가 될 때까지 확인하고 실패 시 예외를 발생시킵니다."""
    lambda_client = _client("lambda")
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            response = lambda_client.get_function(FunctionName=function_name)
        except ClientError:
            time.sleep(0.5)
            continue

        state = response["Configuration"].get("State")
        if state == "Active":
            return
        if state == "Failed":
            reason = response["Configuration"].get("StateReason", "")
            raise AssertionError(f"Lambda {function_name} failed to initialize: {reason}")
        time.sleep(0.5)

    raise AssertionError(f"Lambda {function_name} did not become active within {timeout_seconds} seconds")


def _wait_for_execution(stepfunctions, execution_arn: str, timeout_seconds: int = 30) -> Dict[str, str]:
    """실행이 완료될 때까지 대기한 뒤 최종 describe_execution 응답을 반환합니다."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        response = stepfunctions.describe_execution(executionArn=execution_arn)
        status = response["status"]
        if status not in ("RUNNING", "QUEUED"):
            return response
        time.sleep(0.5)
    raise AssertionError(f"Execution {execution_arn} did not complete within {timeout_seconds} seconds")


def _collect_history_events(stepfunctions, execution_arn: str) -> List[Dict[str, Dict[str, str]]]:
    """지정된 실행의 모든 이력 이벤트를 조회합니다."""
    events: List[Dict[str, Dict[str, str]]] = []
    next_token: str | None = None
    while True:
        kwargs = {"executionArn": execution_arn, "maxResults": 100}
        if next_token:
            kwargs["nextToken"] = next_token
        response = stepfunctions.get_execution_history(**kwargs)
        events.extend(response.get("events", []))
        next_token = response.get("nextToken")
        if not next_token:
            break
    return events


@dataclass
class RetryTestEnvironment:
    """정리 및 재사용을 위한 LocalStack 리소스 식별자 모음입니다."""

    lambda_name: str
    lambda_arn: str
    state_machine_arn: str


@pytest.fixture(scope="module")
def retry_environment() -> Iterable[RetryTestEnvironment]:
    """LocalStack에 재시도 검증용 Lambda와 상태 머신을 준비합니다."""
    _ensure_localstack()

    lambda_name = f"glue-retry-stub-{uuid.uuid4().hex[:8]}"
    state_machine_name = f"retry-test-{uuid.uuid4().hex[:8]}"

    lambda_arn = _create_retry_stub_lambda(lambda_name)
    _wait_for_lambda_active(lambda_name)

    stepfunctions = _client("stepfunctions")
    definition = {
        "Comment": "재시도 동작 검증",
        "StartAt": "InvokeGlueStub",
        "States": {
            "InvokeGlueStub": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": lambda_arn,
                    "Payload": {
                        "attempt_id.$": "$.attempt_id",
                        "max_failures.$": "$.max_failures",
                    },
                },
                "Retry": [
                    {
                        "ErrorEquals": ["GlueConcurrentRunsExceededException"],
                        "IntervalSeconds": 1,
                        "BackoffRate": 2.0,
                        "MaxAttempts": 3,
                    }
                ],
                "ResultPath": "$.lambda_result",
                "Next": "Succeeded",
            },
            "Succeeded": {"Type": "Succeed"},
        },
    }
    state_machine_arn = stepfunctions.create_state_machine(
        name=state_machine_name,
        definition=json.dumps(definition),
        roleArn=f"arn:aws:iam::{LOCALSTACK_ACCOUNT_ID}:role/StepFunctionsExecutionRole",
    )["stateMachineArn"]

    env = RetryTestEnvironment(
        lambda_name=lambda_name,
        lambda_arn=lambda_arn,
        state_machine_arn=state_machine_arn,
    )

    try:
        yield env
    finally:
        # 최선의 방식으로 생성된 리소스를 정리한다.
        with contextlib.suppress(Exception):
            _client("stepfunctions").delete_state_machine(stateMachineArn=env.state_machine_arn)
        with contextlib.suppress(Exception):
            _client("lambda").delete_function(FunctionName=env.lambda_name)


@pytest.mark.integration
@pytest.mark.localstack
def test_retry_eventually_succeeds(retry_environment: RetryTestEnvironment) -> None:
    """
    Given: 최대 두 번까지 실패하도록 구성된 상태 머신
    When: 실행 완료까지 대기
    Then: 세 번째 시도에서 성공하고 TaskFailed 이벤트가 두 번만 기록됨
    """
    # Given: 최대 실패 횟수가 2인 입력으로 상태 머신 실행
    stepfunctions = _client("stepfunctions")

    attempt_id = str(uuid.uuid4())
    payload = {"attempt_id": attempt_id, "max_failures": 2}

    execution_arn = stepfunctions.start_execution(
        stateMachineArn=retry_environment.state_machine_arn,
        input=json.dumps(payload),
        name=f"success-run-{uuid.uuid4().hex[:8]}",
    )["executionArn"]

    # When: 실행이 완료될 때까지 대기
    result = _wait_for_execution(stepfunctions, execution_arn)
    assert result["status"] == "SUCCEEDED"

    # Then: 두 번 실패 후 세 번째 시도에서 성공해야 함
    events = _collect_history_events(stepfunctions, execution_arn)
    failure_events = [event for event in events if event["type"] == "TaskFailed"]
    assert len(failure_events) == 2, "Expected two failed attempts prior to success"

    success_events = [event for event in events if event["type"] == "TaskSucceeded"]
    assert success_events, "Expected a Lambda success event"
    success_output = json.loads(success_events[0]["taskSucceededEventDetails"]["output"])
    payload = success_output["Payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert payload["attempts"] == 3, "Lambda should report the successful attempt count"


@pytest.mark.integration
@pytest.mark.localstack
def test_retry_hits_max_attempts_and_fails(retry_environment: RetryTestEnvironment) -> None:
    """
    Given: 허용 실패 횟수가 재시도 한도를 초과하도록 설정된 상태 머신
    When: 실행 완료까지 대기
    Then: 재시도 예산이 모두 소진되고 ExecutionFailed 이벤트가 발생
    """
    # Given: 최대 실패 횟수가 5인 입력으로 상태 머신 실행
    stepfunctions = _client("stepfunctions")

    attempt_id = str(uuid.uuid4())
    payload = {"attempt_id": attempt_id, "max_failures": 5}

    execution_arn = stepfunctions.start_execution(
        stateMachineArn=retry_environment.state_machine_arn,
        input=json.dumps(payload),
        name=f"failure-run-{uuid.uuid4().hex[:8]}",
    )["executionArn"]

    # When: 실행이 완료될 때까지 대기
    result = _wait_for_execution(stepfunctions, execution_arn)
    assert result["status"] == "FAILED"

    # Then: 재시도 한도를 모두 사용하고 ExecutionFailed 이벤트가 발생해야 함
    events = _collect_history_events(stepfunctions, execution_arn)
    failure_events = [event for event in events if event["type"] == "TaskFailed"]
    assert len(failure_events) == 4, "Expected failures to consume the entire retry budget"

    execution_failed = [event for event in events if event["type"] == "ExecutionFailed"]
    assert execution_failed, "Execution should fail after exceeding retry attempts"
    details = execution_failed[0]["executionFailedEventDetails"]
    assert details["error"] == "GlueConcurrentRunsExceededException"


@pytest.mark.integration
@pytest.mark.localstack
def test_manifest_map_processes_all_entries_once() -> None:
    """
    Given: 10개의 manifest 항목과 순차 실행으로 구성된 상태 머신
    When: Map 상태 실행이 완료될 때까지 대기
    Then: 각 manifest가 정확히 한 번씩 성공하고 추가 실패가 없어야 함
    """
    _ensure_localstack()
    stepfunctions = _client("stepfunctions")

    lambda_name = f"manifest-map-stub-{uuid.uuid4().hex[:8]}"
    lambda_arn = _create_manifest_stub_lambda(lambda_name)
    _wait_for_lambda_active(lambda_name)

    state_machine_name = f"manifest-map-test-{uuid.uuid4().hex[:8]}"
    definition = {
        "Comment": "순차 manifest Map 처리",
        "StartAt": "ProcessManifestList",
        "States": {
            "ProcessManifestList": {
                "Type": "Map",
                "ItemsPath": "$.manifest_keys",
                "MaxConcurrency": 1,
                "Iterator": {
                    "StartAt": "InvokeManifestProcessor",
                    "States": {
                        "InvokeManifestProcessor": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::lambda:invoke",
                            "Parameters": {
                                "FunctionName": lambda_arn,
                                "Payload": {
                                    "manifest_key.$": "$.manifest_key",
                                },
                            },
                            "ResultPath": "$.result",
                            "End": True,
                        }
                    },
                },
                "ResultPath": "$.map_results",
                "Next": "Success",
            },
            "Success": {"Type": "Succeed"},
        },
    }

    state_machine_arn = stepfunctions.create_state_machine(
        name=state_machine_name,
        definition=json.dumps(definition),
        roleArn=f"arn:aws:iam::{LOCALSTACK_ACCOUNT_ID}:role/StepFunctionsExecutionRole",
    )["stateMachineArn"]

    manifest_count = 10
    manifest_items = [{"manifest_key": f"market/prices/day={day:02d}.manifest"} for day in range(1, manifest_count + 1)]

    # Given: 10개의 manifest 항목을 입력으로 준비
    execution_arn = stepfunctions.start_execution(
        stateMachineArn=state_machine_arn,
        input=json.dumps({"manifest_keys": manifest_items}),
        name=f"manifest-run-{uuid.uuid4().hex[:8]}",
    )["executionArn"]

    try:
        # When: 상태 머신 실행이 완료될 때까지 대기
        result = _wait_for_execution(stepfunctions, execution_arn, timeout_seconds=120)
        assert result["status"] == "SUCCEEDED"

        # Then: 모든 manifest가 정확히 한 번씩 성공해야 함
        events = _collect_history_events(stepfunctions, execution_arn)
        assert not [event for event in events if event["type"] == "TaskFailed"], "Map processing should not fail"

        output = json.loads(result["output"])
        results = output["map_results"]
        assert len(results) == manifest_count
        for entry, expected in zip(results, manifest_items):
            processed = entry["result"]
            payload = processed.get("Payload", processed)
            if isinstance(payload, str):
                payload = json.loads(payload)
            assert payload["manifest_key"] == expected["manifest_key"]
            assert payload["calls"] == 1, "Each manifest should be processed exactly once"
    finally:
        with contextlib.suppress(Exception):
            stepfunctions.delete_state_machine(stateMachineArn=state_machine_arn)
        with contextlib.suppress(Exception):
            _client("lambda").delete_function(FunctionName=lambda_name)
