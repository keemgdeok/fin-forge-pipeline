from __future__ import annotations

import importlib
import json
from dataclasses import dataclass
from typing import Any, Dict, List

import pytest
from botocore.exceptions import ClientError

from shared.ingestion.manifests import ManifestEntry

handler = importlib.import_module("src.lambda.functions.batch_completion_trigger.handler")


def _serialize_value(value: Any) -> Dict[str, Any]:
    if isinstance(value, str):
        return {"S": value}
    if isinstance(value, (int, float)):
        return {"N": str(value)}
    if isinstance(value, list):
        return {"L": [_serialize_value(item) for item in value]}
    if isinstance(value, dict):
        return {"M": {k: _serialize_value(v) for k, v in value.items()}}
    if value is None:
        return {"NULL": True}
    raise TypeError(f"Unsupported value type: {type(value)}")


def _build_record(new_image: Dict[str, Any], old_image: Dict[str, Any] | None = None) -> Dict[str, Any]:
    return {
        "eventID": "evt-1",
        "eventName": "MODIFY",
        "dynamodb": {
            "NewImage": {k: _serialize_value(v) for k, v in new_image.items()},
            "OldImage": {k: _serialize_value(v) for k, v in (old_image or {}).items()},
        },
    }


@dataclass
class FakeStepFunctionsClient:
    should_fail: bool = False

    def __post_init__(self) -> None:
        self.started: List[Dict[str, Any]] = []

    def start_execution(self, *, stateMachineArn: str, input: str) -> Dict[str, str]:
        if self.should_fail:
            raise ClientError(
                {"Error": {"Code": "ServiceUnavailableException", "Message": "failed"}},
                "StartExecution",
            )

        payload = json.loads(input)
        self.started.append({"arn": stateMachineArn, "input": payload})
        return {"executionArn": "arn:aws:states:region:acct:execution:123"}


class FakeDynamoTable:
    def __init__(self, fail_lock: bool = False) -> None:
        self.fail_lock = fail_lock
        self.calls: List[Dict[str, Any]] = []

    def update_item(self, **kwargs: Any) -> Dict[str, Any]:
        self.calls.append(kwargs)
        expression = kwargs.get("ConditionExpression") or ""
        if "attribute_not_exists" in expression and self.fail_lock:
            raise ClientError(
                {"Error": {"Code": "ConditionalCheckFailedException", "Message": "locked"}},
                "UpdateItem",
            )
        return {}


class FakeDynamoResource:
    def __init__(self, table: FakeDynamoTable) -> None:
        self.table = table
        self.requested_table_name: str | None = None

    def Table(self, name: str) -> FakeDynamoTable:  # noqa: N802 - matches boto3 interface
        self.requested_table_name = name
        return self.table


@pytest.fixture(autouse=True)
def _env(monkeypatch) -> None:
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("STATE_MACHINE_ARN", "arn:aws:states:region:acct:stateMachine:demo")
    monkeypatch.setenv("RAW_BUCKET", "dev-raw")
    monkeypatch.setenv("BATCH_TRACKER_TABLE", "dev-batch-tracker")
    monkeypatch.setenv("MANIFEST_BASENAME", "_batch")
    monkeypatch.setenv("MANIFEST_SUFFIX", ".manifest.json")
    monkeypatch.setenv("CATALOG_UPDATE_DEFAULT", "on_schema_change")
    monkeypatch.setenv("DEFAULT_DOMAIN", "market")
    monkeypatch.setenv("DEFAULT_TABLE_NAME", "prices")
    monkeypatch.setenv("DEFAULT_INTERVAL", "1d")
    monkeypatch.setenv("DEFAULT_DATA_SOURCE", "yahoo_finance")
    monkeypatch.setenv("DEFAULT_FILE_TYPE", "json")


def test_stream_record_starts_execution(monkeypatch) -> None:
    """
    Given: 상태가 complete로 전환된 DynamoDB 스트림 레코드
    When: batch_completion_trigger main 호출
    Then: Step Functions 실행, processed 1
    """
    fake_table = FakeDynamoTable()
    fake_resource = FakeDynamoResource(fake_table)
    fake_sfn = FakeStepFunctionsClient()

    monkeypatch.setattr(
        handler, "collect_manifest_entries", lambda **_: [ManifestEntry("2024-01-01", "key", "tracker")]
    )
    monkeypatch.setattr(handler.boto3, "resource", lambda service: fake_resource if service == "dynamodb" else None)
    monkeypatch.setattr(handler.boto3, "client", lambda service: fake_sfn if service == "stepfunctions" else None)

    event = {
        "Records": [
            _build_record(
                {
                    "pk": "batch-1",
                    "status": "complete",
                    "meta_domain": "market",
                    "meta_table_name": "prices",
                    "meta_interval": "1d",
                    "meta_data_source": "yahoo_finance",
                },
                {"status": "processing"},
            )
        ]
    }

    result = handler.main(event, None)

    assert result["processed"] == 1
    assert result["batchItemFailures"] == []
    assert fake_sfn.started, "Step Functions execution must be started"
    started_input = fake_sfn.started[0]["input"]
    assert started_input["batch_id"] == "batch-1"
    assert started_input["manifest_keys"][0]["ds"] == "2024-01-01"
    assert fake_table.calls[0]["ConditionExpression"].startswith("attribute_not_exists")


def test_skip_when_lock_already_exists(monkeypatch) -> None:
    """
    Given: ConditionalCheckFailedException을 유발하는 스트림 레코드
    When: batch_completion_trigger 처리
    Then: Step Functions 미호출, processed 0
    """
    fake_table = FakeDynamoTable(fail_lock=True)
    fake_resource = FakeDynamoResource(fake_table)
    fake_sfn = FakeStepFunctionsClient()

    monkeypatch.setattr(handler, "collect_manifest_entries", lambda **_: [ManifestEntry("2024-01-01", "key", None)])
    monkeypatch.setattr(handler.boto3, "resource", lambda service: fake_resource if service == "dynamodb" else None)
    monkeypatch.setattr(handler.boto3, "client", lambda service: fake_sfn if service == "stepfunctions" else None)

    event = {
        "Records": [
            _build_record(
                {
                    "pk": "batch-2",
                    "status": "complete",
                },
                {"status": "processing"},
            )
        ]
    }

    result = handler.main(event, None)

    assert result["processed"] == 0
    assert result["batchItemFailures"] == []
    assert not fake_sfn.started, "No Step Functions execution should be started when lock fails"


def test_step_functions_failure_marks_batch_failure(monkeypatch) -> None:
    """
    Given: Step Functions start_execution 실패를 유발하는 스트림 레코드
    When: batch_completion_trigger 처리
    Then: batchItemFailures에 레코드 ID, DynamoDB 업데이트 롤백
    """
    fake_table = FakeDynamoTable()
    fake_resource = FakeDynamoResource(fake_table)
    fake_sfn = FakeStepFunctionsClient(should_fail=True)

    monkeypatch.setattr(handler, "collect_manifest_entries", lambda **_: [ManifestEntry("2024-01-01", "key", None)])
    monkeypatch.setattr(handler.boto3, "resource", lambda service: fake_resource if service == "dynamodb" else None)
    monkeypatch.setattr(handler.boto3, "client", lambda service: fake_sfn if service == "stepfunctions" else None)

    event = {
        "Records": [
            _build_record(
                {
                    "pk": "batch-3",
                    "status": "complete",
                },
                {"status": "processing"},
            )
        ]
    }

    result = handler.main(event, None)

    assert result["processed"] == 0
    assert result["batchItemFailures"] == [{"itemIdentifier": "evt-1"}]
    # There should be three update attempts: lock acquisition, reset on failure
    assert len(fake_table.calls) >= 2
