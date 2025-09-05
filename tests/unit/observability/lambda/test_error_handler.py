import os
import runpy


def _load_module():
    return runpy.run_path("src/lambda/functions/error_handler/handler.py")


class _Sns:
    def __init__(self):
        self.published = []

    def publish(self, **kwargs):
        self.published.append(kwargs)
        return {"MessageId": "mid-123"}


class _CW:
    def __init__(self):
        self.metrics = []

    def put_metric_data(self, **kwargs):
        self.metrics.append(kwargs)
        return {}


class _Boto:
    def __init__(self, sns, cw):
        self._sns = sns
        self._cw = cw

    def client(self, name: str, region_name=None):
        if name == "sns":
            return self._sns
        if name == "cloudwatch":
            return self._cw
        raise ValueError(name)


def test_error_handler_success(monkeypatch):
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["ERROR_TOPIC_ARN"] = "arn:sns:dev:topic"
    mod = _load_module()

    sns = _Sns()
    cw = _CW()
    monkeypatch.setitem(mod["main"].__globals__, "boto3", _Boto(sns, cw))

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


def test_error_handler_missing_fields(monkeypatch):
    os.environ["ENVIRONMENT"] = "dev"
    mod = _load_module()
    monkeypatch.setitem(mod["main"].__globals__, "boto3", _Boto(_Sns(), _CW()))
    resp = mod["main"]({"error": {"error_message": "oops"}}, None)
    assert resp["statusCode"] == 400
