"""변환 상태 머신이 manifest 목록을 순차적으로 처리하는지 검증합니다.

로컬 Step Functions 실행 대신 CDK 합성을 통해 생성된 정의를 확인하여,

* Map 상태가 `$.manifest_keys`를 대상으로 존재하는지,
* `MaxConcurrency`가 환경 설정값(기본 1)을 반영하는지,
* Preflight → Glue → Success 순서의 반복 구성이 유지되는지 확인합니다.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict

import pytest
from aws_cdk import App, Stack, aws_s3 as s3
from aws_cdk.assertions import Template

from infrastructure.config.environments.dev import dev_config
from infrastructure.pipelines.daily_prices_data.processing_stack import DailyPricesDataProcessingStack


@dataclass
class SharedStorageStub:
    """Minimal shared storage construct required by the processing stack."""

    raw_bucket: s3.Bucket
    curated_bucket: s3.Bucket
    artifacts_bucket: s3.Bucket


def _build_processing_stack(config_override: Dict[str, Any] | None = None) -> DailyPricesDataProcessingStack:
    app = App()

    shared_stack = Stack(app, "SharedStorageStack")
    shared_storage = SharedStorageStub(
        raw_bucket=s3.Bucket(shared_stack, "RawBucket"),
        curated_bucket=s3.Bucket(shared_stack, "CuratedBucket"),
        artifacts_bucket=s3.Bucket(shared_stack, "ArtifactsBucket"),
    )

    config: Dict[str, Any] = dict(dev_config)
    if config_override:
        config.update(config_override)

    # Ensure the processing workflow synthesizes (even if dev config disables it)
    config["enable_processing_orchestration"] = True

    processing_stack = DailyPricesDataProcessingStack(
        app,
        "DailyPricesProcessingStackUnderTest",
        environment="dev",
        config=config,
        shared_storage_stack=shared_storage,
        lambda_execution_role_arn="arn:aws:iam::123456789012:role/lambda-exec",
        glue_execution_role_arn="arn:aws:iam::123456789012:role/glue-exec",
        step_functions_execution_role_arn="arn:aws:iam::123456789012:role/sfn-exec",
    )
    return processing_stack


def _render_definition(definition_prop: Any) -> str:
    """Resolve CloudFormation tokens (Fn::Join/Sub/Ref) into a JSON string."""
    if isinstance(definition_prop, str):
        return definition_prop

    if isinstance(definition_prop, dict):
        if "Fn::Join" in definition_prop:
            joiner, parts = definition_prop["Fn::Join"]
            rendered_parts = [_render_definition(part) for part in parts]
            return str(joiner).join(rendered_parts)
        if "Ref" in definition_prop:
            return f"${{{definition_prop['Ref']}}}"
        if "Fn::GetAtt" in definition_prop:
            attr = definition_prop["Fn::GetAtt"]
            if isinstance(attr, list):
                target = ".".join(str(segment) for segment in attr)
            else:
                target = str(attr)
            return f"${{{target}}}"
        if "Fn::Sub" in definition_prop:
            template = definition_prop["Fn::Sub"]
            if isinstance(template, str):
                return template
            if isinstance(template, list) and template:
                return str(template[0])

    raise TypeError(f"Unsupported CloudFormation structure: {definition_prop}")


def _load_state_machine_definition(template: Template) -> Dict[str, Any]:
    state_machines = template.find_resources("AWS::StepFunctions::StateMachine")
    assert state_machines, "Expected at least one state machine to be synthesized"

    state_machine_def = next(iter(state_machines.values()))
    definition_prop = state_machine_def["Properties"]["DefinitionString"]
    definition_str = _render_definition(definition_prop)
    return json.loads(definition_str)


@pytest.mark.integration
def test_manifest_map_sequential_configuration() -> None:
    """기본 설정에서 Map 상태가 manifest를 순차 처리하는지 검증합니다."""

    # Given: 기본 환경 설정으로 생성한 DailyPrices 처리 스택
    processing_stack = _build_processing_stack()
    template = Template.from_stack(processing_stack)

    # When: 합성된 상태 머신 정의를 로드할 때
    definition = _load_state_machine_definition(template)

    # Then: Map 상태가 순차 처리 구성을 유지해야 함
    map_state = definition["States"]["ProcessManifestList"]
    assert map_state["Type"] == "Map"
    assert map_state["ItemsPath"] == "$.manifest_keys"
    assert map_state["MaxConcurrency"] == dev_config["sfn_max_concurrency"]  # defaults to 1

    iterator = map_state["Iterator"]
    states = iterator["States"]
    assert list(states.keys()) == ["PreflightDailyPrices", "ProcessDailyPrices", "ManifestSuccess"]

    preflight = states["PreflightDailyPrices"]
    process = states["ProcessDailyPrices"]
    success = states["ManifestSuccess"]

    assert preflight["Type"] == "Task"
    assert process["Type"] == "Task"
    assert success["Type"] == "Succeed"
    assert preflight["Next"] == "ProcessDailyPrices"
    assert process["Next"] == "ManifestSuccess"
    assert "Next" not in success


@pytest.mark.integration
def test_manifest_map_respects_overridden_concurrency() -> None:
    """`sfn_max_concurrency`를 덮어썼을 때 정의에 반영되는지 검증합니다."""

    # Given: `sfn_max_concurrency` 값을 3으로 설정한 상태 머신 정의
    processing_stack = _build_processing_stack({"sfn_max_concurrency": 3})
    template = Template.from_stack(processing_stack)

    definition = _load_state_machine_definition(template)

    map_state = definition["States"]["ProcessManifestList"]
    # Then: Map 상태의 `MaxConcurrency`가 3으로 반영되어야 함
    assert map_state["MaxConcurrency"] == 3
