from aws_cdk import App
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
        "enable_processing_orchestration": False,
    }


def _fake_python_function(scope, id, **kwargs):
    # Avoid bundling during synth where PythonFunction is used
    from aws_cdk import aws_lambda as lambda_

    return lambda_.Function(
        scope,
        id,
        runtime=lambda_.Runtime.PYTHON_3_12,
        handler="index.handler",
        code=lambda_.Code.from_inline("def handler(event, context): return {}"),
    )


def test_glue_job_defaults_and_schema_fingerprint_path(monkeypatch) -> None:
    app = App()
    cfg = _base_config()

    # Patch PythonFunction to avoid bundling errors
    monkeypatch.setattr(ps, "PythonFunction", _fake_python_function, raising=False)

    shared = SharedStorageStack(app, "SharedStorageGlueDefaults", environment="dev", config=cfg)
    proc = ps.CustomerDataProcessingStack(
        app,
        "ProcStackGlueDefaults",
        environment="dev",
        config=cfg,
        shared_storage_stack=shared,
        lambda_execution_role_arn="arn:aws:iam::111122223333:role/lambda",
        glue_execution_role_arn="arn:aws:iam::111122223333:role/glue",
        step_functions_execution_role_arn="arn:aws:iam::111122223333:role/sfn",
    )

    template = Template.from_stack(proc)
    jobs = template.find_resources("AWS::Glue::Job")
    assert jobs, "Glue Job must exist"
    job = next(iter(jobs.values()))

    props = job["Properties"]
    assert props["Timeout"] == 30, "Timeout must be 30 minutes per spec"
    assert props["MaxRetries"] == 1, "MaxRetries must be 1 per spec"

    default_args = props["DefaultArguments"]
    assert (
        "_schema/latest.json" in default_args["--schema_fingerprint_s3_uri"]
    ), "Schema fingerprint path must use _schema/latest.json"
