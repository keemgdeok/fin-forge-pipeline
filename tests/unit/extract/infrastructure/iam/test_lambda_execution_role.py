import json
from typing import cast

from aws_cdk import App, Stack
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
    LambdaExecutionRoleConstruct(stack, "LambdaRole", env_name="dev", config=_cfg())
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
    LambdaExecutionRoleConstruct(stack, "LambdaRoleAssets", env_name="prod", config=_cfg())
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
