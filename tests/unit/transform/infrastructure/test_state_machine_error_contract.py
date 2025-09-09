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


def test_simplified_state_machine_has_essential_error_handling(monkeypatch) -> None:
    """Test that simplified state machine still has proper error handling.

    After removing 4 Lambda functions, verify that:
    1. Error normalization still works
    2. Preflight skip branch still exists
    3. Glue ETL error catching is in place
    4. No references to removed Lambda functions
    """
    app = App()
    cfg = _base_config()

    # Prevent bundling by replacing PythonFunction
    monkeypatch.setattr(ps, "PythonFunction", _fake_python_function, raising=False)

    shared = SharedStorageStack(app, "SharedStorageErr", environment="dev", config=cfg)
    proc = ps.CustomerDataProcessingStack(
        app,
        "ProcStackErr",
        environment="dev",
        config=cfg,
        shared_storage_stack=shared,
        lambda_execution_role_arn="arn:aws:iam::111122223333:role/lambda",
        glue_execution_role_arn="arn:aws:iam::111122223333:role/glue",
        step_functions_execution_role_arn="arn:aws:iam::111122223333:role/sfn",
    )

    template = Template.from_stack(proc)
    tpl = template.to_json()

    # Essential error handling must still exist
    assert "NormalizeAndFail" in tpl
    assert '"ok": false' in tpl
    assert '"error.$": "$.error"' in tpl

    # Preflight skip branch must exist
    assert "PreflightSkipOrError" in tpl
    assert '"$.error.code"' in tpl and "IDEMPOTENT_SKIP" in tpl

    # Verify simplified workflow - no quality/schema check steps
    assert "QualityCheckCustomerData" not in tpl, "Quality check Lambda should be removed"
    assert "SchemaCheckCustomerData" not in tpl, "Schema check Lambda should be removed"
    assert "BuildDateArray" not in tpl, "Build dates Lambda should be removed"

    # Essential components should remain
    assert "PreflightCustomerData" in tpl, "Preflight Lambda must remain"
    assert "ProcessCustomerData" in tpl, "Glue ETL job must remain"
    assert "StartCrawler" in tpl, "Crawler task must remain"
