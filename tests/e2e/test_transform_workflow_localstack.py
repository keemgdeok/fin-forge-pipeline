from __future__ import annotations

import os

import pytest

from tests.e2e.utils.env import LocalStackConfig
from tests.e2e.utils.ingestion import run_ingestion
from tests.e2e.utils.localstack_ops import list_objects
from tests.e2e.utils.stubs import stub_yahoo_finance
from tests.e2e.utils.workflow import (
    WorkflowDeployment,
    collect_execution_history,
    collect_failure_events,
    start_state_machine_execution,
    wait_for_execution,
)


LIGHT_MANIFEST_COUNT = int(os.environ.get("E2E_WORKFLOW_LIGHT_MANIFESTS", "5"))
FULL_MANIFEST_COUNT = int(os.environ.get("E2E_WORKFLOW_FULL_MANIFESTS", "100"))
RUN_FULL_WORKFLOW = os.environ.get("E2E_ENABLE_FULL_WORKFLOW", "").lower() in {"1", "true", "yes"}


@pytest.mark.e2e
@pytest.mark.parametrize(
    "manifest_count",
    [
        pytest.param(LIGHT_MANIFEST_COUNT, id="light"),
        pytest.param(
            FULL_MANIFEST_COUNT,
            id="full",
            marks=pytest.mark.skipif(
                not RUN_FULL_WORKFLOW, reason="Set E2E_ENABLE_FULL_WORKFLOW=1 to run full workflow test"
            ),
        ),
    ],
)
def test_transform_state_machine_localstack(
    monkeypatch: pytest.MonkeyPatch,
    localstack_config: LocalStackConfig,
    transform_workflow: WorkflowDeployment,
    manifest_count: int,
) -> None:
    """
    Given: LocalStack에 Step Functions 워크플로와 인제스트 데이터가 준비됨
    When: manifest 개수에 맞춰 상태 머신 실행
    Then: 각 manifest에 대해 Preflight가 호출되고 Curated 산출물이 생성되어야 함
    """
    config = localstack_config
    stub_yahoo_finance(monkeypatch, days=manifest_count)

    ingestion = run_ingestion(
        monkeypatch,
        config,
        domain="market",
        table_name="prices",
        interval="1d",
        data_source="yahoo_finance",
        indicator_layer="technical_indicator",
        symbols=["AAPL", "MSFT", "GOOGL"],
        manifest_days=manifest_count,
    )

    workflow_input = {
        "manifest_items": [
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
            for key in ingestion.manifest_keys
        ],
        "compacted_layer": "compacted",
        "curated_layer": "adjusted",
        "indicator_layer": ingestion.indicator_layer,
    }

    execution_arn = start_state_machine_execution(config, transform_workflow.state_machine_arn, workflow_input)
    result = wait_for_execution(config, execution_arn)
    history = collect_execution_history(config, execution_arn)

    if result.get("status") != "SUCCEEDED":
        details = collect_failure_events(history)
        pytest.fail(f"Workflow execution failed: {result.get('status')}\n{details}")

    preflight_entries = [
        event
        for event in history
        if event.get("type") == "TaskStateEntered"
        and event.get("stateEnteredEventDetails", {}).get("name") == "Preflight"
    ]
    assert len(preflight_entries) == manifest_count

    curated_objects = list_objects(config, ingestion.curated_bucket, f"{ingestion.domain}/{ingestion.table_name}/")
    parquet_files = [key for key in curated_objects if key.endswith(".parquet")]
    assert len(parquet_files) >= manifest_count
