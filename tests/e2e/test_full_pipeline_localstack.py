from __future__ import annotations

import json
import os
import runpy
from typing import Any, Dict

import pytest

from tests.e2e.utils import localstack_ops
from tests.e2e.utils.env import LocalStackConfig, ensure_load_contracts_path
from tests.e2e.utils.ingestion import IngestionContext, run_ingestion
from tests.e2e.utils.stubs import stub_yahoo_finance
from tests.e2e.utils.workflow import (
    WorkflowDeployment,
    collect_execution_history,
    collect_failure_events,
    start_state_machine_execution,
    wait_for_execution,
)


EXPECTED_MANIFEST_COUNT = int(os.environ.get("E2E_FULL_PIPELINE_MANIFESTS", "20"))
PIPELINE_LIGHT_MODE = os.environ.get("E2E_PIPELINE_LIGHT_MODE", "").lower() in {"1", "true", "yes"}


def _build_workflow_input(
    ingestion: IngestionContext,
    manifest_keys: list[str],
    *,
    compacted_layer: str,
    curated_layer: str,
) -> Dict[str, Any]:
    manifest_items = [
        {
            "manifest_key": key,
            "domain": ingestion.domain,
            "table_name": ingestion.table_name,
            "interval": ingestion.interval,
            "data_source": ingestion.data_source,
            "raw_bucket": ingestion.raw_bucket,
            "curated_bucket": ingestion.curated_bucket,
            "artifacts_bucket": ingestion.artifacts_bucket,
            "catalog_update": "force",
        }
        for key in manifest_keys
    ]
    return {
        "manifest_items": manifest_items,
        "compacted_layer": compacted_layer,
        "curated_layer": curated_layer,
    }


@pytest.mark.e2e
def test_full_pipeline_end_to_end_with_localstack(
    monkeypatch: pytest.MonkeyPatch,
    localstack_config: LocalStackConfig,
    transform_workflow: WorkflowDeployment,
) -> None:
    """
    Given: LocalStack 환경과 인제스트/트랜스폼 파이프라인이 준비됨
    When: 전체 파이프라인을 실행하고 Load 이벤트까지 처리
    Then: 상태 머신이 성공하고 S3/SQS 산출물이 예상대로 생성되어야 함
    """
    if PIPELINE_LIGHT_MODE:
        pytest.skip("PIPELINE_LIGHT_MODE 활성화 시 실제 워크플로우가 실행되지 않습니다")
    config = localstack_config
    ensure_load_contracts_path()
    stub_yahoo_finance(monkeypatch, days=EXPECTED_MANIFEST_COUNT)

    domain = "market"
    table_name = "prices"
    interval = "1d"
    data_source = "yahoo_finance"

    ingestion = run_ingestion(
        monkeypatch,
        config,
        domain=domain,
        table_name=table_name,
        interval=interval,
        data_source=data_source,
        symbols=["AAPL", "MSFT", "GOOGL"],
        manifest_days=EXPECTED_MANIFEST_COUNT,
    )

    workflow_input = _build_workflow_input(
        ingestion,
        ingestion.manifest_keys,
        compacted_layer="compacted",
        curated_layer="adjusted",
    )

    execution_arn = start_state_machine_execution(config, transform_workflow.state_machine_arn, workflow_input)
    execution_result = wait_for_execution(config, execution_arn)
    history = collect_execution_history(config, execution_arn)

    if execution_result.get("status") != "SUCCEEDED":
        details = collect_failure_events(history)
        pytest.fail(f"State machine execution failed: {execution_result.get('status')}\n{details}")

    preflight_entries = [
        event
        for event in history
        if event.get("type") == "TaskStateEntered"
        and event.get("stateEnteredEventDetails", {}).get("name") == "Preflight"
    ]
    assert len(preflight_entries) == EXPECTED_MANIFEST_COUNT, (
        f"Expected {EXPECTED_MANIFEST_COUNT} Preflight invocations, found {len(preflight_entries)}"
    )

    first_manifest = ingestion.manifest_keys[0]
    first_ds = localstack_ops.parse_manifest_ds(first_manifest)
    curated_objects = localstack_ops.list_objects(config, ingestion.curated_bucket, f"{domain}/{table_name}/")
    assert curated_objects, "Curated dataset should contain output objects"
    parquet_candidates = [key for key in curated_objects if key.endswith(".parquet")]
    assert parquet_candidates, "Expected at least one curated parquet object"
    curated_object_key = parquet_candidates[0]

    s3_client = config.client("s3")
    head_resp = s3_client.head_object(Bucket=ingestion.curated_bucket, Key=curated_object_key)
    curated_size = int(head_resp["ContentLength"])

    curated_prefix = curated_object_key.rsplit("/", 1)[0]
    summary_key = f"{curated_prefix}/dataset.json"
    summary_resp = s3_client.get_object(Bucket=ingestion.curated_bucket, Key=summary_key)
    summary_payload = json.loads(summary_resp["Body"].read().decode("utf-8"))
    assert summary_payload["domain"] == domain
    assert summary_payload["table_name"] == table_name
    assert summary_payload["ds"] == first_ds
    assert summary_payload["records"] >= 1

    load_event = {
        "source": "aws.s3",
        "detail-type": "Object Created",
        "detail": {
            "bucket": {"name": ingestion.curated_bucket},
            "object": {
                "key": curated_object_key,
                "size": curated_size,
            },
        },
    }

    queue_map = {domain: ingestion.load_queue_url}
    monkeypatch.setenv("LOAD_QUEUE_MAP", json.dumps(queue_map))
    monkeypatch.setenv("PRIORITY_MAP", json.dumps({domain: "1"}))
    monkeypatch.setenv("MIN_FILE_SIZE_BYTES", "1")
    monkeypatch.setenv("ALLOWED_LAYERS", json.dumps(["adjusted"]))

    load_publisher_module = runpy.run_path("src/lambda/functions/load_event_publisher/handler.py")
    publisher_result = load_publisher_module["main"](load_event, None)
    assert publisher_result["status"] == "SUCCESS"

    load_messages = localstack_ops.receive_all_messages(config, ingestion.load_queue_url)
    assert load_messages, "Load queue should receive published message"
    first_message = load_messages[0]
    message_body = json.loads(first_message["Body"])
    assert message_body["domain"] == domain
    assert message_body["table_name"] == table_name
    assert message_body["layer"] == "adjusted"
    assert message_body["bucket"] == ingestion.curated_bucket
    assert message_body["key"] == curated_object_key
    assert message_body["file_size"] == curated_size

    attributes = first_message.get("MessageAttributes", {})
    assert attributes.get("Priority", {}).get("StringValue") == "1"
    assert attributes.get("ContentType", {}).get("StringValue") == "application/json"

    localstack_ops.delete_messages(config, ingestion.load_queue_url, load_messages)

    artifacts_keys = localstack_ops.list_objects(config, ingestion.artifacts_bucket, f"{domain}/{table_name}/")
    latest_schema = next((key for key in artifacts_keys if key.endswith("_schema/latest.json")), None)
    assert latest_schema, "Schema latest.json should exist"
    schema_obj = s3_client.get_object(Bucket=ingestion.artifacts_bucket, Key=latest_schema)
    schema_payload = json.loads(schema_obj["Body"].read().decode("utf-8"))
    assert "columns" in schema_payload
    assert schema_payload.get("hash")
