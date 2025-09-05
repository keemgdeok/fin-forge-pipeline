import os
import runpy


def _load_module():
    return runpy.run_path("src/lambda/functions/notification_handler/handler.py")


class _SNS:
    def __init__(self):
        self.publishes = []

    def publish(self, **kwargs):
        self.publishes.append(kwargs)
        return {"MessageId": "mid-999"}


class _SES:
    def __init__(self):
        self.sent = []

    def send_email(self, **kwargs):
        self.sent.append(kwargs)
        return {"MessageId": "em-1"}


class _Boto:
    def __init__(self, sns, ses):
        self._sns = sns
        self._ses = ses

    def client(self, name: str, region_name=None):
        if name == "sns":
            return self._sns
        if name == "ses":
            return self._ses
        raise ValueError(name)


def test_notification_sns_only(monkeypatch):
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["NOTIFICATION_TOPIC_ARN"] = "arn:sns:dev:topic"
    mod = _load_module()
    sns = _SNS()
    ses = _SES()
    monkeypatch.setitem(mod["main"].__globals__, "boto3", _Boto(sns, ses))

    event = {
        "pipeline_name": "customer-data",
        "domain": "market",
        "table_name": "prices",
        "status": "SUCCEEDED",
    }

    resp = mod["main"](event, None)
    assert resp["statusCode"] == 200
    assert len(sns.publishes) == 1
    # No email for non-critical status
    assert len(ses.sent) == 0


def test_notification_email_on_critical(monkeypatch):
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["NOTIFICATION_TOPIC_ARN"] = "arn:sns:dev:topic"
    os.environ["SOURCE_EMAIL"] = "noreply@example.com"
    os.environ["ADMIN_EMAILS"] = "ops@example.com"

    mod = _load_module()
    sns = _SNS()
    ses = _SES()
    monkeypatch.setitem(mod["main"].__globals__, "boto3", _Boto(sns, ses))

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
    mod = _load_module()
    monkeypatch.setitem(mod["main"].__globals__, "boto3", _Boto(_SNS(), _SES()))
    resp = mod["main"]({"domain": "x"}, None)
    assert resp["statusCode"] == 400
