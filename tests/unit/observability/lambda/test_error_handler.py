import os
import runpy
from tests.fixtures.clients import SnsStub, CloudWatchStub, BotoStub


def test_error_handler_success(monkeypatch) -> None:
    """
    Given: 오류 이벤트와 SNS/CW 스텁
    When: 에러 핸들러를 실행하면
    Then: SNS 발행과 CloudWatch 지표 기록이 정상 수행되어야 함
    """
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["ERROR_TOPIC_ARN"] = "arn:sns:dev:topic"
    mod = runpy.run_path("src/lambda/functions/error_handler/handler.py")

    sns = SnsStub()
    cw = CloudWatchStub()
    monkeypatch.setitem(mod["main"].__globals__, "boto3", BotoStub(sns=sns, cloudwatch=cw))

    event = {
        "error": {
            "source": "validator",
            "error_type": "ValidationError",
            "error_message": "invalid schema",
            "severity": "ERROR",
            "context": {"domain": "market", "table_name": "prices"},
        }
    }

    resp = mod["main"](event, None)
    assert resp["statusCode"] == 200
    body = resp["body"]
    assert body["notification_sent"] is True
    assert len(sns.published) == 1
    assert len(cw.metrics) >= 1


def test_error_handler_missing_fields(monkeypatch) -> None:
    """
    Given: 필수 필드가 누락된 오류 이벤트
    When: 에러 핸들러를 실행하면
    Then: 400 상태 코드가 반환되어야 함
    """
    os.environ["ENVIRONMENT"] = "dev"
    mod = runpy.run_path("src/lambda/functions/error_handler/handler.py")
    monkeypatch.setitem(mod["main"].__globals__, "boto3", BotoStub(sns=SnsStub(), cloudwatch=CloudWatchStub()))
    resp = mod["main"]({"error": {"error_message": "oops"}}, None)
    assert resp["statusCode"] == 400
