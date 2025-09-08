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
        "backfill_max_concurrency": 3,
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


def test_backfill_map_exists_with_concurrency(monkeypatch) -> None:
    app = App()
    cfg = _base_config()

    # Avoid bundling
    monkeypatch.setattr(ps, "PythonFunction", _fake_python_function, raising=False)

    shared = SharedStorageStack(app, "SharedStorageBackfill", environment="dev", config=cfg)
    proc = ps.CustomerDataProcessingStack(
        app,
        "ProcStackBackfill",
        environment="dev",
        config=cfg,
        shared_storage_stack=shared,
        lambda_execution_role_arn="arn:aws:iam::111122223333:role/lambda",
        glue_execution_role_arn="arn:aws:iam::111122223333:role/glue",
        step_functions_execution_role_arn="arn:aws:iam::111122223333:role/sfn",
    )

    template = Template.from_stack(proc)
    tpl = template.to_json()

    # Verify Map state exists and uses expected settings
    assert '"Type": "Map"' in tpl
    assert '"BackfillMap"' in tpl
    assert '"MaxConcurrency": 3' in tpl
    # ItemsPath should come from $.dates
    assert '"ItemsPath": "$.dates"' in tpl
    # Top-level choice should include backfill selector
    assert '"BackfillOrSingle"' in tpl
