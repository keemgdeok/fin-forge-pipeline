from aws_cdk import (
    App,
    Duration,
)
from aws_cdk.assertions import Template

from infrastructure.core.shared_storage_stack import SharedStorageStack
from infrastructure.pipelines.customer_data import processing_stack as ps


def _base_config():
    return {
        "s3_retention_days": 30,
        "auto_delete_objects": True,
        "log_retention_days": 14,
        "ingestion_domain": "market",
        "ingestion_table_name": "prices",
        "ingestion_file_format": "json",
        "enable_processing_orchestration": True,
        "processing_triggers": [
            {
                "domain": "market",
                "table_name": "prices",
                "file_type": "json",
                "suffixes": [".json", ".csv"],
            }
        ],
    }


def test_raw_bucket_eventbridge_enabled():
    app = App()
    cfg = _base_config()
    cfg["enable_processing_orchestration"] = False
    shared = SharedStorageStack(app, "SharedStorage", environment="dev", config=cfg)
    template = Template.from_stack(shared)

    # 하나 이상의 버킷에서 EventBridge 통합이 활성화되어야 함 (RAW)
    # CDK는 CloudFormation에 EventBridgeConfiguration 빈 객체를 생성합니다.
    # 일부 버전에서는 EventBridgeEnabled 플래그 없이 빈 객체만 존재합니다.
    buckets = template.find_resources("AWS::S3::Bucket")
    customs = template.find_resources("Custom::S3BucketNotifications")

    bucket_has_eb = any(
        (r.get("Properties", {}).get("NotificationConfiguration", {}).get("EventBridgeConfiguration") is not None)
        for r in buckets.values()
    )

    custom_has_eb = any(
        (r.get("Properties", {}).get("NotificationConfiguration", {}).get("EventBridgeConfiguration") is not None)
        for r in customs.values()
    )

    assert bucket_has_eb or custom_has_eb, (
        "EventBridgeConfiguration must exist on the bucket or on the" " Custom::S3BucketNotifications resource"
    )


def test_eventbridge_rule_has_prefix_and_suffixes(monkeypatch):
    app = App()
    cfg = _base_config()
    cfg["enable_processing_orchestration"] = True

    # PythonFunction 번들링 회피를 위해 inline Lambda로 대체
    def _fake_python_function(scope, id, **kwargs):
        from aws_cdk import aws_lambda as lambda_

        return lambda_.Function(
            scope,
            id,
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.handler",
            code=lambda_.Code.from_inline("def handler(event, context): return {}"),
            memory_size=kwargs.get("memory_size", 128),
            timeout=kwargs.get("timeout", Duration.seconds(10)),
            log_retention=kwargs.get("log_retention"),
            role=kwargs.get("role"),
            layers=kwargs.get("layers", []),
            environment=kwargs.get("environment", {}),
        )

    monkeypatch.setattr(ps, "PythonFunction", _fake_python_function, raising=False)

    shared = SharedStorageStack(app, "SharedStorage2", environment="dev", config=cfg)
    proc = ps.CustomerDataProcessingStack(
        app,
        "ProcStack",
        environment="dev",
        config=cfg,
        shared_storage_stack=shared,
        lambda_execution_role_arn="arn:aws:iam::111122223333:role/lambda",
        glue_execution_role_arn="arn:aws:iam::111122223333:role/glue",
        step_functions_execution_role_arn="arn:aws:iam::111122223333:role/sfn",
    )

    template = Template.from_stack(proc)

    rules = template.find_resources("AWS::Events::Rule")
    assert rules, "Events Rule must be defined"

    # 최소 하나의 룰에 prefix와 두 개의 suffix(.json, .csv)가 함께 있어야 함
    def _has_expected_key_filters(rsrc):
        pattern = rsrc.get("Properties", {}).get("EventPattern", {})
        detail = pattern.get("detail", {})
        obj = detail.get("object", {})
        keys = obj.get("key", [])
        # keys는 [{prefix: 'market/prices/', suffix: '.json'}, {...'.csv'}]
        suffixes = {k.get("suffix") for k in keys if isinstance(k, dict)}
        prefixes = {k.get("prefix") for k in keys if isinstance(k, dict)}
        return ".json" in suffixes and ".csv" in suffixes and "market/prices/" in prefixes

    assert any(_has_expected_key_filters(r) for r in rules.values())


def test_eventbridge_rules_disabled_by_flag(monkeypatch):
    app = App()
    cfg = _base_config()
    cfg["enable_processing_orchestration"] = False

    # Patch PythonFunction as 위 테스트와 동일
    def _fake_python_function(scope, id, **kwargs):
        from aws_cdk import aws_lambda as lambda_

        return lambda_.Function(
            scope,
            id,
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.handler",
            code=lambda_.Code.from_inline("def handler(event, context): return {}"),
        )

    monkeypatch.setattr(ps, "PythonFunction", _fake_python_function, raising=False)

    shared = SharedStorageStack(app, "SharedStorage3", environment="dev", config=cfg)
    proc = ps.CustomerDataProcessingStack(
        app,
        "ProcStack2",
        environment="dev",
        config=cfg,
        shared_storage_stack=shared,
        lambda_execution_role_arn="arn:aws:iam::111122223333:role/lambda",
        glue_execution_role_arn="arn:aws:iam::111122223333:role/glue",
        step_functions_execution_role_arn="arn:aws:iam::111122223333:role/sfn",
    )

    template = Template.from_stack(proc)
    rules = template.find_resources("AWS::Events::Rule")
    assert not rules, "Events Rule must not exist when orchestration disabled"
