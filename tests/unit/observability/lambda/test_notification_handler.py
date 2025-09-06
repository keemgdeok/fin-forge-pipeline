import os
import runpy
from tests.fixtures.clients import SnsStub, SesStub, BotoStub


def test_notification_sns_only(monkeypatch):
    """
    Given: 정상(SUCCEEDED) 상태의 파이프라인 알림 이벤트와 SNS 토픽 ARN
    When: 알림 핸들러가 이벤트를 처리하면
    Then: SNS 게시 1건이 발생하고 이메일은 전송되지 않아야 함
    """
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["NOTIFICATION_TOPIC_ARN"] = "arn:sns:dev:topic"
    mod = runpy.run_path("src/lambda/functions/notification_handler/handler.py")
    sns = SnsStub()
    ses = SesStub()
    monkeypatch.setitem(mod["main"].__globals__, "boto3", BotoStub(sns=sns, ses=ses))

    event = {
        "pipeline_name": "customer-data",
        "domain": "market",
        "table_name": "prices",
        "status": "SUCCEEDED",
    }

    resp = mod["main"](event, None)
    assert resp["statusCode"] == 200
    assert len(sns.published) == 1
    # No email for non-critical status
    assert len(ses.sent) == 0


def test_notification_email_on_critical(monkeypatch):
    """
    Given: 오류(ERROR) 상태의 알림과 이메일 설정
    When: 알림 핸들러가 이벤트를 처리하면
    Then: SNS 게시와 함께 이메일이 1건 전송되어야 함
    """
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["NOTIFICATION_TOPIC_ARN"] = "arn:sns:dev:topic"
    os.environ["SOURCE_EMAIL"] = "noreply@example.com"
    os.environ["ADMIN_EMAILS"] = "ops@example.com"

    mod = runpy.run_path("src/lambda/functions/notification_handler/handler.py")
    sns = SnsStub()
    ses = SesStub()
    monkeypatch.setitem(mod["main"].__globals__, "boto3", BotoStub(sns=sns, ses=ses))

    event = {
        "pipeline_name": "customer-data",
        "domain": "market",
        "table_name": "prices",
        "status": "ERROR",
        "error_details": {"error_type": "GlueError", "error_message": "job failed"},
    }

    resp = mod["main"](event, None)
    assert resp["statusCode"] == 200
    # Email should be sent for critical status
    assert len(ses.sent) == 1


def test_notification_missing_fields(monkeypatch):
    """
    Given: 필수 필드가 누락된 이벤트
    When: 알림 핸들러가 이벤트를 검증하면
    Then: 400 상태 코드가 반환되어야 함
    """
    mod = runpy.run_path("src/lambda/functions/notification_handler/handler.py")
    monkeypatch.setitem(mod["main"].__globals__, "boto3", BotoStub(sns=SnsStub(), ses=SesStub()))
    resp = mod["main"]({"domain": "x"}, None)
    assert resp["statusCode"] == 400
