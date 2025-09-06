import os
import runpy


def test_error_handler_missing_required_fields_returns_400(monkeypatch) -> None:
    """
    Given: source/error_message 누락된 오류 이벤트
    When: 에러 핸들러를 실행하면
    Then: 400 상태 코드가 반환되어야 함
    """
    os.environ["ENVIRONMENT"] = "dev"
    mod = runpy.run_path("src/lambda/functions/error_handler/handler.py")
    resp = mod["main"]({"error": {"severity": "ERROR"}}, None)
    assert resp["statusCode"] == 400


def test_error_handler_skips_sns_when_topic_not_configured(monkeypatch) -> None:
    """
    Given: ERROR_TOPIC_ARN 미설정
    When: 에러 핸들러를 실행하면
    Then: SNS 발행을 스킵하되 200 응답이 반환되어야 함
    """
    os.environ["ENVIRONMENT"] = "dev"
    os.environ.pop("ERROR_TOPIC_ARN", None)
    mod = runpy.run_path("src/lambda/functions/error_handler/handler.py")

    class _SNS:
        def publish(self, **kwargs):
            raise AssertionError("should not be called")

    class _CW:
        def put_metric_data(self, **kwargs):
            return {}

    class _Boto:
        def client(self, name: str, region_name=None):
            if name == "sns":
                return _SNS()
            if name == "cloudwatch":
                return _CW()
            raise AssertionError(name)

    monkeypatch.setitem(mod["main"].__globals__, "boto3", _Boto())

    event = {
        "error": {
            "source": "validator",
            "error_message": "oops",
            "severity": "ERROR",
        }
    }
    resp = mod["main"](event, None)
    assert resp["statusCode"] == 200
