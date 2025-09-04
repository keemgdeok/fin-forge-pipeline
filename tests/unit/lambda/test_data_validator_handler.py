import os
import runpy


def load_validator_module():
    return runpy.run_path("src/lambda/functions/data_validator/handler.py")


class _StubStepFunctionsClient:
    def start_execution(self, **kwargs):
        return {"executionArn": "arn:aws:states:us-east-1:111111111111:execution:sm:exec"}


class _StubValidator:
    def __init__(self):
        self.stepfunctions_client = _StubStepFunctionsClient()

    def validate_data_comprehensive(self, config):
        return {"overall_valid": True, "checks": [], "errors": []}

    def trigger_glue_etl_job(self, job_name, arguments=None):
        return "jr-1234"


def test_validator_success_path(monkeypatch):
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["RAW_BUCKET"] = "raw-bucket-dev"
    os.environ["CURATED_BUCKET"] = "curated-bucket-dev"

    mod = load_validator_module()
    # Patch the symbol in the handler's function globals to avoid boto3 usage
    monkeypatch.setitem(mod["main"].__globals__, "DataValidator", _StubValidator)

    event = {
        "source_bucket": "raw-bucket-dev",
        "source_key": "market/prices/2024-01-01.csv",
        "table_name": "prices",
        "domain": "market",
        "file_type": "csv",
        "glue_job_config": {"job_name": "dev-customer-data-etl"},
        "step_function_arn": "arn:aws:states:us-east-1:111111111111:stateMachine:dev-customer-data-processing",
    }

    resp = mod["main"](event, None)
    assert resp["validation_passed"] is True
    assert resp["etl_job_run_id"] == "jr-1234"
    assert resp["domain"] == "market"


def test_validator_missing_params(monkeypatch):
    mod = load_validator_module()
    resp = mod["main"]({"domain": "market"}, None)
    assert resp["validation_passed"] is False
    assert "error" in resp
