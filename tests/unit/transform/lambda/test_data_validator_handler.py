import os
import runpy


def load_validator_module():
    return runpy.run_path("src/lambda/functions/data_validator/handler.py")


class _StubValidator:
    def __init__(self):
        class _SFN:
            def __getattr__(self, name):
                raise AssertionError("StepFunctions client should not be used")

        # Guard: if 코드가 잘못되어 SFN을 호출하면 테스트가 실패하도록 함
        self.stepfunctions_client = _SFN()

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
    }

    resp = mod["main"](event, None)
    assert resp["validation_passed"] is True
    assert resp["etl_job_run_id"] == "jr-1234"
    assert resp["domain"] == "market"
    # 더 이상 Step Functions 실행 결과를 반환하지 않음
    assert "step_function_execution_arn" not in resp


def test_validator_missing_params(monkeypatch):
    mod = load_validator_module()
    resp = mod["main"]({"domain": "market"}, None)
    assert resp["validation_passed"] is False
    assert "error" in resp
