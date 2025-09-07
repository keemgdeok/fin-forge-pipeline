import os
import runpy


def test_notification_handler_missing_required_fields_returns_400() -> None:
    """
    Given: 필수 필드(pipeline_name/domain/status) 누락
    When: 알림 핸들러 실행
    Then: 400 상태 코드
    """
    mod = runpy.run_path("src/lambda/functions/notification_handler/handler.py")
    resp = mod["main"]({"domain": "x"}, None)
    assert resp["statusCode"] == 400


def test_notification_handler_sns_skipped_when_topic_missing(monkeypatch) -> None:
    """
    Given: NOTIFICATION_TOPIC_ARN 미설정
    When: 정상 알림 처리
    Then: SNS는 건너뛰고 200 응답
    """
    os.environ["ENVIRONMENT"] = "dev"
    os.environ.pop("NOTIFICATION_TOPIC_ARN", None)
    mod = runpy.run_path("src/lambda/functions/notification_handler/handler.py")

    class _SNS:
        def publish(self, **kwargs):
            raise AssertionError("should not be called")

    class _SES:
        def send_email(self, **kwargs):
            # non-critical status -> no email
            raise AssertionError("should not be called")

    class _Boto:
        def client(self, name: str, region_name=None):
            if name == "sns":
                return _SNS()
            if name == "ses":
                return _SES()
            raise AssertionError(name)

    monkeypatch.setitem(mod["main"].__globals__, "boto3", _Boto())

    event = {
        "pipeline_name": "customer-data",
        "domain": "market",
        "table_name": "prices",
        "status": "SUCCEEDED",
    }
    resp = mod["main"](event, None)
    assert resp["statusCode"] == 200
