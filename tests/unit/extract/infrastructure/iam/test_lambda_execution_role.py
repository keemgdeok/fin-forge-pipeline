import json
from typing import cast

from aws_cdk import App, Stack, RemovalPolicy
from aws_cdk import aws_s3 as s3, aws_dynamodb as dynamodb
from aws_cdk.assertions import Template

from infrastructure.config.types import EnvironmentConfig
from infrastructure.core.iam.lambda_execution_role import LambdaExecutionRoleConstruct


def _cfg() -> EnvironmentConfig:
    return cast(EnvironmentConfig, {})


def _find_role(template: Template, role_suffix: str) -> dict:
    resources = template.find_resources("AWS::IAM::Role")
    for res in resources.values():
        role_name = res.get("Properties", {}).get("RoleName", "")
        if role_name.endswith(role_suffix):
            return res
    raise AssertionError(f"Role with suffix '{role_suffix}' not found")


def _policy_statements(role: dict, policy_name: str) -> list[dict]:
    for policy in role.get("Properties", {}).get("Policies", []):
        if policy.get("PolicyName") == policy_name:
            document = policy.get("PolicyDocument", {})
            return document.get("Statement", [])
    return []


def _to_list(value) -> list:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    return [value]


def test_lambda_role_has_sqs_send_permissions() -> None:
    """Lambda 실행 역할은 SQS SendMessage 권한을 포함해야 한다."""
    app = App()
    stack = Stack(app, "TestStack")
    raw_bucket = s3.Bucket(stack, "Raw", removal_policy=RemovalPolicy.DESTROY)
    curated_bucket = s3.Bucket(stack, "Curated", removal_policy=RemovalPolicy.DESTROY)
    artifacts_bucket = s3.Bucket(stack, "Artifacts", removal_policy=RemovalPolicy.DESTROY)
    tracker_table = dynamodb.Table(
        stack,
        "Tracker",
        partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
        billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
        removal_policy=RemovalPolicy.DESTROY,
        stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
    )
    LambdaExecutionRoleConstruct(
        stack,
        "LambdaRole",
        env_name="dev",
        config=_cfg(),
        raw_bucket=raw_bucket,
        curated_bucket=curated_bucket,
        artifacts_bucket=artifacts_bucket,
        batch_tracker_table=tracker_table,
    )
    template = Template.from_stack(stack)

    role = _find_role(template, "lambda-role")
    statements = _policy_statements(role, "SqsSendMessage")
    assert statements, "SqsSendMessage policy must exist"

    actions = [action for stmt in statements for action in _to_list(stmt.get("Action"))]
    assert "sqs:SendMessage" in actions
    assert "sqs:SendMessageBatch" in actions


def test_lambda_role_can_read_cdk_asset_bucket() -> None:
    """Lambda 실행 역할은 CDK 자산 버킷에서 스크립트를 읽을 수 있어야 한다."""
    app = App()
    stack = Stack(app, "TestStackAssets")
    raw_bucket = s3.Bucket(stack, "RawAssets", removal_policy=RemovalPolicy.DESTROY)
    curated_bucket = s3.Bucket(stack, "CuratedAssets", removal_policy=RemovalPolicy.DESTROY)
    artifacts_bucket = s3.Bucket(stack, "ArtifactsAssets", removal_policy=RemovalPolicy.DESTROY)
    tracker_table = dynamodb.Table(
        stack,
        "TrackerAssets",
        partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
        billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
        removal_policy=RemovalPolicy.DESTROY,
        stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
    )
    LambdaExecutionRoleConstruct(
        stack,
        "LambdaRoleAssets",
        env_name="prod",
        config=_cfg(),
        raw_bucket=raw_bucket,
        curated_bucket=curated_bucket,
        artifacts_bucket=artifacts_bucket,
        batch_tracker_table=tracker_table,
    )
    template = Template.from_stack(stack)

    role = _find_role(template, "lambda-role")
    statements = _policy_statements(role, "S3Access")
    get_object_statements = [
        stmt
        for stmt in statements
        if "s3:GetObject" in _to_list(stmt.get("Action")) or "s3:GetObjectVersion" in _to_list(stmt.get("Action"))
    ]
    assert get_object_statements, "Lambda S3Access policy must include GetObject actions"

    resources_blob = json.dumps([stmt.get("Resource", []) for stmt in get_object_statements])
    assert "cdk-hnb659fds-assets" in resources_blob, "Lambda role must allow reading CDK asset bucket"
