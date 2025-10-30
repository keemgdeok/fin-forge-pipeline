from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping

from tests.e2e.lambda_stubs import stub_path
from tests.e2e.utils.env import LocalStackConfig
from tests.e2e.utils.zipper import zip_lambda_from_path


@dataclass(frozen=True)
class LambdaStubMetadata:
    """Metadata required to deploy a lambda stub into LocalStack."""

    name: str
    module: Path
    timeout: int = 60


@dataclass(frozen=True)
class LambdaDeployment:
    """Represents a deployed lambda stub."""

    stub_key: str
    function_name: str
    arn: str


@dataclass(frozen=True)
class WorkflowDeployment:
    """Container for a deployed transform workflow."""

    state_machine_arn: str
    lambdas: Dict[str, LambdaDeployment]


TRANSFORM_WORKFLOW_STUBS: Dict[str, LambdaStubMetadata] = {
    "preflight": LambdaStubMetadata(name="preflight", module=stub_path("preflight"), timeout=30),
    "compaction": LambdaStubMetadata(name="compaction", module=stub_path("compaction"), timeout=120),
    "guard": LambdaStubMetadata(name="guard", module=stub_path("guard"), timeout=30),
    "etl": LambdaStubMetadata(name="etl", module=stub_path("etl"), timeout=60),
    "indicators": LambdaStubMetadata(name="indicators", module=stub_path("indicators"), timeout=30),
    "schema": LambdaStubMetadata(name="schema", module=stub_path("schema_decider"), timeout=10),
}


def stub_environment(config: LocalStackConfig) -> Dict[str, str]:
    """Build the base environment variables injected into every lambda stub."""
    return {
        "LOCALSTACK_ENDPOINT": config.internal_endpoint,
        "AWS_REGION": config.region,
        "AWS_DEFAULT_REGION": config.region,
        "AWS_ACCESS_KEY_ID": config.access_key,
        "AWS_SECRET_ACCESS_KEY": config.secret_key,
    }


def deploy_lambda_stubs(
    config: LocalStackConfig,
    *,
    environment: Mapping[str, str],
    suffix: str,
    stubs: Mapping[str, LambdaStubMetadata] | None = None,
) -> Dict[str, LambdaDeployment]:
    selected_stubs = stubs or TRANSFORM_WORKFLOW_STUBS
    deployments: Dict[str, LambdaDeployment] = {}
    for key, metadata in selected_stubs.items():
        function_name = f"e2e-{metadata.name}-{suffix}-{uuid.uuid4().hex[:6]}"
        arn = create_lambda_function(
            config,
            function_name,
            metadata.module,
            environment=environment,
            timeout=metadata.timeout,
        )
        deployments[key] = LambdaDeployment(stub_key=key, function_name=function_name, arn=arn)
    return deployments


def destroy_lambda_stubs(config: LocalStackConfig, deployments: Iterable[LambdaDeployment]) -> None:
    for deployment in deployments:
        delete_lambda(config, deployment.function_name)


def deploy_transform_workflow(
    config: LocalStackConfig,
    *,
    environment: Mapping[str, str] | None = None,
    stubs: Mapping[str, LambdaStubMetadata] | None = None,
) -> WorkflowDeployment:
    suffix = uuid.uuid4().hex[:8]
    env = dict(environment or stub_environment(config))
    lambda_deployments = deploy_lambda_stubs(config, environment=env, suffix=suffix, stubs=stubs)
    state_machine_name = f"e2e-transform-{suffix}"
    definition = build_state_machine_definition({key: deployment.arn for key, deployment in lambda_deployments.items()})
    state_machine_arn = create_state_machine(config, definition, state_machine_name)
    return WorkflowDeployment(state_machine_arn=state_machine_arn, lambdas=lambda_deployments)


def destroy_transform_workflow(config: LocalStackConfig, deployment: WorkflowDeployment) -> None:
    delete_state_machine(config, deployment.state_machine_arn)
    destroy_lambda_stubs(config, deployment.lambdas.values())


def create_lambda_function(
    config: LocalStackConfig,
    function_name: str,
    module_path: Path,
    *,
    environment: Mapping[str, str],
    timeout: int = 60,
) -> str:
    code = zip_lambda_from_path(module_path)
    client = config.client("lambda")
    client.create_function(
        FunctionName=function_name,
        Runtime="python3.11",
        Role=f"arn:aws:iam::{config.account_id}:role/dummy-role",
        Handler="index.lambda_handler",
        Code={"ZipFile": code},
        Timeout=timeout,
        Environment={"Variables": dict(environment)},
    )
    wait_for_lambda_active(config, function_name)
    return f"arn:aws:lambda:{config.region}:{config.account_id}:function:{function_name}"


def delete_lambda(config: LocalStackConfig, function_name: str) -> None:
    client = config.client("lambda")
    try:
        client.delete_function(FunctionName=function_name)
    except client.exceptions.ResourceNotFoundException:
        pass


def wait_for_lambda_active(config: LocalStackConfig, function_name: str, timeout_seconds: int = 120) -> None:
    client = config.client("lambda")
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            response = client.get_function(FunctionName=function_name)
        except client.exceptions.ResourceNotFoundException:
            time.sleep(0.5)
            continue
        state = response["Configuration"].get("State")
        if state == "Active":
            return
        if state == "Failed":
            reason = response["Configuration"].get("StateReason", "")
            raise RuntimeError(f"Lambda {function_name} failed to initialize: {reason}")
        time.sleep(0.5)
    raise RuntimeError(f"Lambda {function_name} did not become active within {timeout_seconds} seconds")


def create_state_machine(config: LocalStackConfig, definition: Mapping[str, Any], name: str) -> str:
    client = config.client("stepfunctions")
    response = client.create_state_machine(
        name=name,
        definition=json.dumps(definition),
        roleArn=f"arn:aws:iam::{config.account_id}:role/dummy-role",
    )
    return response["stateMachineArn"]


def delete_state_machine(config: LocalStackConfig, state_machine_arn: str) -> None:
    client = config.client("stepfunctions")
    try:
        client.delete_state_machine(stateMachineArn=state_machine_arn)
    except client.exceptions.StateMachineDoesNotExist:
        pass
    except Exception:
        # LocalStack occasionally raises generic exceptions when the state machine is mid-deletion
        pass


def start_state_machine_execution(
    config: LocalStackConfig, state_machine_arn: str, input_data: Mapping[str, Any]
) -> str:
    client = config.client("stepfunctions")
    response = client.start_execution(stateMachineArn=state_machine_arn, input=json.dumps(input_data))
    return response["executionArn"]


def wait_for_execution(config: LocalStackConfig, execution_arn: str, timeout_seconds: int = 300) -> Dict[str, Any]:
    client = config.client("stepfunctions")
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        response = client.describe_execution(executionArn=execution_arn)
        status = response["status"]
        if status not in ("RUNNING", "QUEUED"):
            return response
        time.sleep(1)
    raise TimeoutError(f"Execution {execution_arn} did not finish within {timeout_seconds} seconds")


def collect_execution_history(config: LocalStackConfig, execution_arn: str) -> list[dict[str, Any]]:
    client = config.client("stepfunctions")
    events: list[dict[str, Any]] = []
    next_token: str | None = None
    while True:
        kwargs: Dict[str, Any] = {"executionArn": execution_arn, "maxResults": 100}
        if next_token:
            kwargs["nextToken"] = next_token
        response = client.get_execution_history(**kwargs)
        events.extend(response.get("events", []))
        next_token = response.get("nextToken")
        if not next_token:
            break
    return events


def build_state_machine_definition(lambda_arns: Mapping[str, str]) -> Dict[str, Any]:
    """Construct the transform workflow definition using the provided lambda ARNs."""
    return {
        "Comment": "LocalStack transform workflow for e2e test",
        "StartAt": "ProcessManifestList",
        "States": {
            "ProcessManifestList": {
                "Type": "Map",
                "MaxConcurrency": 3,
                "ItemsPath": "$.manifest_items",
                "Parameters": {
                    "manifest_key.$": "$$.Map.Item.Value.manifest_key",
                    "domain.$": "$$.Map.Item.Value.domain",
                    "table_name.$": "$$.Map.Item.Value.table_name",
                    "interval.$": "$$.Map.Item.Value.interval",
                    "data_source.$": "$$.Map.Item.Value.data_source",
                    "raw_bucket.$": "$$.Map.Item.Value.raw_bucket",
                    "curated_bucket.$": "$$.Map.Item.Value.curated_bucket",
                    "artifacts_bucket.$": "$$.Map.Item.Value.artifacts_bucket",
                    "compacted_layer.$": "$.compacted_layer",
                    "curated_layer.$": "$.curated_layer",
                    "indicator_layer.$": "$.indicator_layer",
                },
                "ResultPath": None,
                "Iterator": {
                    "StartAt": "Preflight",
                    "States": {
                        "Preflight": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::lambda:invoke",
                            "Parameters": {
                                "FunctionName": lambda_arns["preflight"],
                                "Payload.$": "$",
                            },
                            "ResultSelector": {
                                "proceed.$": "$.Payload.proceed",
                                "glue_args.$": "$.Payload.glue_args",
                                "compaction_args.$": "$.Payload.compaction_args",
                                "manifest_key.$": "$.Payload.manifest_key",
                                "ds.$": "$.Payload.ds",
                            },
                            "ResultPath": "$.preflight",
                            "Next": "CheckProceed",
                        },
                        "CheckProceed": {
                            "Type": "Choice",
                            "Choices": [
                                {
                                    "Variable": "$.preflight.proceed",
                                    "BooleanEquals": True,
                                    "Next": "Compaction",
                                }
                            ],
                            "Default": "SkipItem",
                        },
                        "SkipItem": {"Type": "Pass", "Result": {"skipped": True}, "Next": "ItemDone"},
                        "Compaction": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::lambda:invoke",
                            "Parameters": {
                                "FunctionName": lambda_arns["compaction"],
                                "Payload.$": "$",
                            },
                            "ResultSelector": {
                                "copied.$": "$.Payload.copied",
                                "compacted_prefix.$": "$.Payload.compacted_prefix",
                            },
                            "ResultPath": "$.compaction",
                            "Next": "CompactionGuard",
                        },
                        "CompactionGuard": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::lambda:invoke",
                            "Parameters": {
                                "FunctionName": lambda_arns["guard"],
                                "Payload.$": "$",
                            },
                            "ResultSelector": {
                                "shouldProcess.$": "$.Payload.shouldProcess",
                                "prefix.$": "$.Payload.prefix",
                            },
                            "ResultPath": "$.guard",
                            "Next": "ShouldProcess",
                        },
                        "ShouldProcess": {
                            "Type": "Choice",
                            "Choices": [
                                {
                                    "Variable": "$.guard.shouldProcess",
                                    "BooleanEquals": True,
                                    "Next": "Etl",
                                }
                            ],
                            "Default": "ItemDone",
                        },
                        "Etl": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::lambda:invoke",
                            "Parameters": {
                                "FunctionName": lambda_arns["etl"],
                                "Payload.$": "$",
                            },
                            "ResultSelector": {
                                "curated_key.$": "$.Payload.curated_key",
                                "summary_key.$": "$.Payload.summary_key",
                                "schema_key.$": "$.Payload.schema_key",
                            },
                            "ResultPath": "$.etl",
                            "Next": "Indicators",
                        },
                        "Indicators": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::lambda:invoke",
                            "Parameters": {
                                "FunctionName": lambda_arns["indicators"],
                                "Payload.$": "$",
                            },
                            "ResultSelector": {
                                "indicator_key.$": "$.Payload.indicator_key",
                            },
                            "ResultPath": "$.indicators",
                            "Next": "SchemaDecider",
                        },
                        "SchemaDecider": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::lambda:invoke",
                            "Parameters": {
                                "FunctionName": lambda_arns["schema"],
                                "Payload.$": "$",
                            },
                            "ResultSelector": {
                                "shouldRunCrawler.$": "$.Payload.shouldRunCrawler",
                            },
                            "ResultPath": "$.schema",
                            "Next": "ItemDone",
                        },
                        "ItemDone": {"Type": "Succeed"},
                    },
                },
                "Next": "AllManifestsProcessed",
            },
            "AllManifestsProcessed": {"Type": "Succeed"},
        },
    }


def collect_failure_events(history: Iterable[dict[str, Any]]) -> str:
    """Aggregate detailed failure messages from Step Functions events."""
    failure_types = {
        "TaskFailed": "taskFailedEventDetails",
        "MapIterationFailed": "mapIterationFailedEventDetails",
        "ExecutionFailed": "executionFailedEventDetails",
    }
    failures: list[str] = []
    for event in history:
        event_type = event.get("type")
        if event_type not in failure_types:
            continue
        key = failure_types[event_type]
        failures.append(json.dumps(event.get(key, {}), default=str))
    return "\n".join(failures) or "No failure details captured"
