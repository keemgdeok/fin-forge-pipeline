import json
from typing import cast

from aws_cdk import App
from aws_cdk.assertions import Template

from infrastructure.config.types import EnvironmentConfig
from infrastructure.core.security_stack import SecurityStack


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


def test_step_functions_role_invokes_lambda_with_colon_arn_format():
    """
    Step Functions 실행 역할은 람다 ARN을 colon 구분자로 참조해야 한다.
    """
    app = App()
    stack = SecurityStack(app, "SecStack", environment="prod", config=_cfg())
    template = Template.from_stack(stack)

    role = _find_role(template, "stepfunctions-role")
    statements = _policy_statements(role, "LambdaInvoke")
    assert statements, "LambdaInvoke policy must exist"

    resources_blob = json.dumps([stmt.get("Resource", []) for stmt in statements])
    # Ensure colon-delimited Lambda ARN references exist (no trailing '/')
    assert ":function:prod-daily-prices-data-preflight" in resources_blob
    assert ":function:prod-schema-change-decider" in resources_blob
    assert ":function:prod-daily-prices-compaction-guard" in resources_blob
    assert "function/prod" not in resources_blob


def test_step_functions_role_includes_all_required_glue_jobs_and_crawlers():
    """
    Step Functions 실행 역할은 구성된 Glue 잡과 크롤러 권한을 모두 포함해야 한다.
    """
    app = App()
    stack = SecurityStack(app, "SecStack", environment="dev", config=_cfg())
    template = Template.from_stack(stack)

    role = _find_role(template, "stepfunctions-role")
    job_statements = _policy_statements(role, "GlueJobManagement")
    crawler_statements = _policy_statements(role, "GlueCrawlerManagement")

    job_resources_blob = json.dumps([stmt.get("Resource", []) for stmt in job_statements])
    crawler_resources_blob = json.dumps([stmt.get("Resource", []) for stmt in crawler_statements])

    assert ":job/dev-daily-prices-data-etl" in job_resources_blob
    assert ":job/dev-daily-prices-compaction" in job_resources_blob
    assert ":crawler/dev-curated-data-crawler" in crawler_resources_blob


def test_step_functions_role_respects_configuration_overrides():
    """
    구성으로 전달된 자원 이름이 환경 접두어와 함께 정확히 적용되는지 검증한다.
    """
    app = App()
    config = cast(
        EnvironmentConfig,
        {
            "step_functions_lambda_functions": ["custom-mapper"],
            "step_functions_glue_jobs": ["custom-etl"],
            "step_functions_glue_crawlers": ["custom-crawler"],
        },
    )
    stack = SecurityStack(app, "SecStack", environment="staging", config=config)
    template = Template.from_stack(stack)

    role = _find_role(template, "stepfunctions-role")

    lambda_resources_blob = json.dumps([stmt.get("Resource", []) for stmt in _policy_statements(role, "LambdaInvoke")])
    job_resources_blob = json.dumps(
        [stmt.get("Resource", []) for stmt in _policy_statements(role, "GlueJobManagement")]
    )
    crawler_resources_blob = json.dumps(
        [stmt.get("Resource", []) for stmt in _policy_statements(role, "GlueCrawlerManagement")]
    )

    assert ":function:staging-custom-mapper" in lambda_resources_blob
    assert ":job/staging-custom-etl" in job_resources_blob
    assert ":crawler/staging-custom-crawler" in crawler_resources_blob
