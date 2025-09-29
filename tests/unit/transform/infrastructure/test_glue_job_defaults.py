from aws_cdk import App
from aws_cdk.assertions import Template

from infrastructure.core.shared_storage_stack import SharedStorageStack
from infrastructure.pipelines.daily_prices_data import processing_stack as ps


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


def _find_transform_job(template: Template) -> dict:
    jobs = template.find_resources("AWS::Glue::Job")
    assert jobs, "Glue Job must exist"

    for job in jobs.values():
        props = job["Properties"]
        default_args = props.get("DefaultArguments", {})
        if default_args.get("--job-bookmark-option") == "job-bookmark-enable":
            return props

    raise AssertionError("Transform Glue job with job bookmarks enabled not found")


def test_glue_job_defaults_and_schema_fingerprint_path(monkeypatch) -> None:
    app = App()
    cfg = _base_config()

    # Patch PythonFunction to avoid bundling errors
    monkeypatch.setattr(ps, "PythonFunction", _fake_python_function, raising=False)

    shared = SharedStorageStack(app, "SharedStorageGlueDefaults", environment="dev", config=cfg)
    proc = ps.DailyPricesDataProcessingStack(
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
    props = _find_transform_job(template)
    assert props["Timeout"] == 30, "Timeout must be 30 minutes per spec"
    assert props["MaxRetries"] == 1, "MaxRetries must be 1 per spec"
    assert props["GlueVersion"] == "5.0", "Glue version must be 5.0"
    assert props["WorkerType"] == "G.1X", "Worker type must be G.1X"
    # NumberOfWorkers should equal config glue_max_capacity, defaulting to 2 when not provided
    expected_workers = int(cfg.get("glue_max_capacity", 2))
    assert props["NumberOfWorkers"] == expected_workers, "Workers must equal glue_max_capacity"

    default_args = props["DefaultArguments"]
    schema_uri = default_args["--schema_fingerprint_s3_uri"]
    assert default_args.get("--enable-s3-parquet-optimized-committer") == "true"
    assert default_args.get("--job-bookmark-option") == "job-bookmark-enable"

    # Handle CloudFormation references (Fn::Join, Fn::Sub, etc.)
    if isinstance(schema_uri, str):
        assert "_schema/latest.json" in schema_uri, "Schema fingerprint path must use _schema/latest.json"
    elif isinstance(schema_uri, dict):
        # Check for Fn::Join pattern
        if "Fn::Join" in schema_uri:
            join_parts = schema_uri["Fn::Join"][1]  # [1] contains the parts array
            joined_str = "".join(str(part) for part in join_parts)
            assert "_schema/latest.json" in joined_str, "Schema fingerprint path must use _schema/latest.json"
        else:
            # Convert dict to string for other CloudFormation functions
            uri_str = str(schema_uri)
            assert "_schema/latest.json" in uri_str, "Schema fingerprint path must use _schema/latest.json"

    # Command python version
    cmd = props["Command"]
    if isinstance(cmd, dict):
        assert cmd.get("PythonVersion") == "3", "Glue Job must use PythonVersion 3"


def test_glue_job_includes_shared_package_via_extra_py_files(monkeypatch) -> None:
    app = App()
    cfg = _base_config()

    # Patch PythonFunction to avoid bundling errors
    monkeypatch.setattr(ps, "PythonFunction", _fake_python_function, raising=False)

    shared = SharedStorageStack(app, "SharedStorageGlueExtras", environment="dev", config=cfg)
    proc = ps.DailyPricesDataProcessingStack(
        app,
        "ProcStackGlueExtras",
        environment="dev",
        config=cfg,
        shared_storage_stack=shared,
        lambda_execution_role_arn="arn:aws:iam::111122223333:role/lambda",
        glue_execution_role_arn="arn:aws:iam::111122223333:role/glue",
        step_functions_execution_role_arn="arn:aws:iam::111122223333:role/sfn",
    )

    template = Template.from_stack(proc)
    props = _find_transform_job(template)
    default_args = props["DefaultArguments"]
    assert "--extra-py-files" in default_args, "Glue job must include extra py files for shared package"
