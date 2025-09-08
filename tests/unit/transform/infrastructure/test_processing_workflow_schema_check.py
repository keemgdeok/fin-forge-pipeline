from aws_cdk import App, Duration
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
        "catalog_update": "on_schema_change",
        "processing_triggers": [
            {
                "domain": "market",
                "table_name": "prices",
                "file_type": "json",
                "suffixes": [".json", ".csv"],
            }
        ],
    }


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


def test_state_machine_includes_schema_check_and_crawler(monkeypatch) -> None:
    app = App()
    cfg = _base_config()

    # Prevent bundling
    monkeypatch.setattr(ps, "PythonFunction", _fake_python_function, raising=False)

    shared = SharedStorageStack(app, "SharedStorageSchema", environment="dev", config=cfg)
    proc = ps.CustomerDataProcessingStack(
        app,
        "ProcStackSchema",
        environment="dev",
        config=cfg,
        shared_storage_stack=shared,
        lambda_execution_role_arn="arn:aws:iam::111122223333:role/lambda",
        glue_execution_role_arn="arn:aws:iam::111122223333:role/glue",
        step_functions_execution_role_arn="arn:aws:iam::111122223333:role/sfn",
    )

    template = Template.from_stack(proc)
    tpl = template.to_json()

    # The SFN definition should include preflight, pass Glue arguments dynamically, and conditionally start crawler
    assert "PreflightCustomerData" in tpl
    assert '"Arguments.$": "$.glue_args"' in tpl
    assert "aws-sdk:glue:startCrawler" in tpl
    assert "SchemaCheckCustomerData" in tpl
