import os
import runpy
from datetime import datetime, timezone


def _load_module():
    return runpy.run_path("src/lambda/functions/data_ingestion/handler.py")


class _StubS3Client:
    def __init__(self, keycount: int = 0):
        self.keycount = keycount
        self.put_calls = []

    def list_objects_v2(self, **kwargs):
        return {"KeyCount": self.keycount}

    def put_object(self, **kwargs):
        self.put_calls.append(kwargs)
        return {"ETag": "stub"}


class _StubBoto3:
    def __init__(self, s3_client: _StubS3Client):
        self._s3 = s3_client

    def client(self, name: str):
        if name == "s3":
            return self._s3
        if name == "stepfunctions":
            class _SFN:
                def start_execution(self, **kwargs):
                    return {"executionArn": "arn:states:stub"}

            return _SFN()
        raise ValueError(name)


class _StubRecord:
    def __init__(self, symbol: str, ts: datetime):
        self.symbol = symbol
        self.timestamp = ts
        self.open = 1.0
        self.high = 2.0
        self.low = 0.5
        self.close = 1.5
        self.volume = 100.0

    def as_dict(self):
        return {
            "symbol": self.symbol,
            "timestamp": self.timestamp.isoformat(),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
        }


class _StubYahooClient:
    def __init__(self, recs):
        self._recs = recs

    def fetch_prices(self, symbols, period, interval):
        return self._recs


def test_ingestion_s3_write_json(monkeypatch):
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["RAW_BUCKET"] = "raw-bucket-dev"

    mod = _load_module()
    now = datetime.now(timezone.utc)
    recs = [_StubRecord("AAPL", now), _StubRecord("MSFT", now)]

    # Patch external deps
    s3_stub = _StubS3Client(keycount=0)
    monkeypatch.setitem(mod["main"].__globals__, "boto3", _StubBoto3(s3_stub))
    monkeypatch.setitem(mod["main"].__globals__, "YahooFinanceClient", lambda: _StubYahooClient(recs))

    event = {
        "data_source": "yahoo_finance",
        "data_type": "prices",
        "symbols": ["AAPL", "MSFT"],
        "period": "1mo",
        "interval": "1d",
        "domain": "market",
        "table_name": "prices",
        "file_format": "json",
    }

    resp = mod["main"](event, None)
    assert resp["statusCode"] == 200
    body = resp["body"]
    assert body["processed_records"] == 2
    assert len(body["written_keys"]) >= 1
    # Verify S3 put was called and content type
    assert len(s3_stub.put_calls) >= 1
    assert s3_stub.put_calls[0]["ContentType"] == "application/json"


def test_ingestion_idempotency_skips(monkeypatch):
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["RAW_BUCKET"] = "raw-bucket-dev"

    mod = _load_module()
    now = datetime.now(timezone.utc)
    recs = [_StubRecord("AAPL", now)]

    s3_stub = _StubS3Client(keycount=1)  # prefix already exists
    monkeypatch.setitem(mod["main"].__globals__, "boto3", _StubBoto3(s3_stub))
    monkeypatch.setitem(mod["main"].__globals__, "YahooFinanceClient", lambda: _StubYahooClient(recs))

    event = {
        "data_source": "yahoo_finance",
        "data_type": "prices",
        "symbols": ["AAPL"],
        "period": "1mo",
        "interval": "1d",
        "domain": "market",
        "table_name": "prices",
        "file_format": "csv",
    }

    resp = mod["main"](event, None)
    assert resp["statusCode"] == 200
    body = resp["body"]
    # processed_records counts fetched rows, but no writes due to idempotency
    assert body["processed_records"] == 1
    assert body["written_keys"] == []
    assert len(s3_stub.put_calls) == 0
