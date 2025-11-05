import json
from typing import cast

from aws_cdk import App, Stack, RemovalPolicy
from aws_cdk import aws_s3 as s3
from aws_cdk.assertions import Template

from infrastructure.config.types import EnvironmentConfig
from infrastructure.core.iam.glue_execution_role import GlueExecutionRoleConstruct


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


def test_glue_role_can_read_cdk_asset_bucket() -> None:
    """Glue 실행 역할은 CDK 자산 버킷에서 스크립트를 읽을 수 있어야 한다."""
    app = App()
    stack = Stack(app, "GlueStack")
    raw_bucket = s3.Bucket(stack, "Raw", removal_policy=RemovalPolicy.DESTROY)
    curated_bucket = s3.Bucket(stack, "Curated", removal_policy=RemovalPolicy.DESTROY)
    artifacts_bucket = s3.Bucket(stack, "Artifacts", removal_policy=RemovalPolicy.DESTROY)
    GlueExecutionRoleConstruct(
        stack,
        "GlueRole",
        env_name="prod",
        config=_cfg(),
        raw_bucket=raw_bucket,
        curated_bucket=curated_bucket,
        artifacts_bucket=artifacts_bucket,
    )
    template = Template.from_stack(stack)

    role = _find_role(template, "glue-role")
    statements = _policy_statements(role, "S3DataAccess")
    read_statements = [
        stmt
        for stmt in statements
        if "s3:GetObject" in _to_list(stmt.get("Action")) or "s3:GetObjectVersion" in _to_list(stmt.get("Action"))
    ]
    assert read_statements, "Glue S3DataAccess policy must include GetObject actions"

    resources_blob = json.dumps([stmt.get("Resource", []) for stmt in read_statements])
    assert "cdk-hnb659fds-assets" in resources_blob, "Glue role must allow reading CDK asset bucket"


def test_glue_role_locks_temp_list_prefix() -> None:
    """Glue 역할은 artifacts 버킷의 temp prefix에 대해서만 ListBucket을 허용해야 한다."""
    app = App()
    stack = Stack(app, "GlueStackTemp")
    raw_bucket = s3.Bucket(stack, "RawTemp", removal_policy=RemovalPolicy.DESTROY)
    curated_bucket = s3.Bucket(stack, "CuratedTemp", removal_policy=RemovalPolicy.DESTROY)
    artifacts_bucket = s3.Bucket(stack, "ArtifactsTemp", removal_policy=RemovalPolicy.DESTROY)
    GlueExecutionRoleConstruct(
        stack,
        "GlueRoleTemp",
        env_name="dev",
        config=_cfg(),
        raw_bucket=raw_bucket,
        curated_bucket=curated_bucket,
        artifacts_bucket=artifacts_bucket,
    )
    template = Template.from_stack(stack)

    role = _find_role(template, "glue-role")
    statements = _policy_statements(role, "S3DataAccess")

    temp_list_statements = [
        stmt
        for stmt in statements
        if "s3:ListBucket" in _to_list(stmt.get("Action"))
        and stmt.get("Condition")
        and "temp" in json.dumps(stmt.get("Condition"))
    ]
    assert temp_list_statements, "Glue role must restrict ListBucket on artifacts bucket to temp prefix"


def test_glue_role_can_manage_schema_objects() -> None:
    """Glue 역할은 artifacts/_schema 경로에 대해 읽기/쓰기를 허용해야 한다."""
    app = App()
    stack = Stack(app, "GlueStackSchema")
    config = cast(
        EnvironmentConfig,
        {"processing_triggers": [{"domain": "market", "table_name": "prices"}]},
    )
    raw_bucket = s3.Bucket(stack, "RawSchema", removal_policy=RemovalPolicy.DESTROY)
    curated_bucket = s3.Bucket(stack, "CuratedSchema", removal_policy=RemovalPolicy.DESTROY)
    artifacts_bucket = s3.Bucket(stack, "ArtifactsSchema", removal_policy=RemovalPolicy.DESTROY)
    GlueExecutionRoleConstruct(
        stack,
        "GlueRoleSchema",
        env_name="prod",
        config=config,
        raw_bucket=raw_bucket,
        curated_bucket=curated_bucket,
        artifacts_bucket=artifacts_bucket,
    )
    template = Template.from_stack(stack)

    role = _find_role(template, "glue-role")
    statements = _policy_statements(role, "S3DataAccess")

    resources_blob = json.dumps([stmt.get("Resource", []) for stmt in statements])
    assert "market/prices/_schema" in resources_blob, "Schema path must be present in policy resources"

    write_statements = [stmt for stmt in statements if "s3:PutObject" in _to_list(stmt.get("Action"))]
    assert write_statements, "Glue role must allow PutObject"
    assert any("market/prices/_schema" in json.dumps(stmt.get("Resource", [])) for stmt in write_statements)

    read_statements = [
        stmt
        for stmt in statements
        if "s3:GetObject" in _to_list(stmt.get("Action")) or "s3:GetObjectVersion" in _to_list(stmt.get("Action"))
    ]
    assert read_statements, "Glue role must allow GetObject"
    assert any("market/prices/_schema" in json.dumps(stmt.get("Resource", [])) for stmt in read_statements)
