import os
import json
import runpy
from datetime import datetime, timezone


def _load_orchestrator_module():
    return runpy.run_path("src/lambda/functions/ingestion_orchestrator/handler.py")


def _load_worker_module():
    return runpy.run_path("src/lambda/functions/ingestion_worker/handler.py")


class _SQSStub:
    def __init__(self):
        self.batches = []

    def send_message_batch(self, **kwargs):
        self.batches.append(kwargs["Entries"])
        return {"Successful": [{"Id": e["Id"]} for e in kwargs["Entries"]]}


class _S3Stub:
    def __init__(self, existing_symbol: str | None = None):
        self.put_calls = []
        self.existing_symbol = existing_symbol or ""

    def list_objects_v2(self, **kwargs):
        prefix = kwargs.get("Prefix", "")
        # If prefix contains the existing_symbol, pretend an object already exists
        if self.existing_symbol and f"symbol={self.existing_symbol}/" in prefix:
            return {"KeyCount": 1}
        return {"KeyCount": 0}

    def put_object(self, **kwargs):
        self.put_calls.append(kwargs)
        return {"ETag": "stub"}


class _BotoStub:
    def __init__(self, sqs: _SQSStub | None = None, s3: _S3Stub | None = None):
        self._sqs = sqs
        self._s3 = s3

    def client(self, name: str):
        if name == "sqs" and self._sqs is not None:
            return self._sqs
        if name == "s3" and self._s3 is not None:
            return self._s3
        raise ValueError(name)


class _Rec:
    def __init__(self, symbol: str, ts: datetime):
        self.symbol = symbol
        self.timestamp = ts
        self.open = 1.0
        self.high = 1.0
        self.low = 1.0
        self.close = 1.0
        self.volume = 1.0

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


class _YFStub:
    def __init__(self, symbols):
        self._symbols = symbols

    def fetch_prices(self, symbols, period, interval):
        ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
        out = []
        for s in symbols:
            out.append(_Rec(s, ts))
        return out


def _sqs_records_from_batches(batches):
    records = []
    mid = 0
    for batch in batches:
        for e in batch:
            records.append({"messageId": f"m-{mid}", "body": e["MessageBody"]})
            mid += 1
    return {"Records": records}


def test_e2e_basic_flow(monkeypatch):
    # Orchestrator environment
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["QUEUE_URL"] = "https://sqs.example/queue"
    os.environ["CHUNK_SIZE"] = "2"
    os.environ["SQS_SEND_BATCH_SIZE"] = "10"

    mod_orc = _load_orchestrator_module()
    sqs = _SQSStub()
    # Patch boto3 in orchestrator
    monkeypatch.setitem(mod_orc["main"].__globals__, "boto3", _BotoStub(sqs=sqs))

    event = {
        "symbols": ["AAPL", "MSFT", "GOOG"],
        "domain": "market",
        "table_name": "prices",
        "period": "1mo",
        "interval": "1d",
        "file_format": "json",
    }

    resp = mod_orc["main"](event, None)
    assert resp["published"] == 2  # 3 symbols -> chunks of 2 -> 2 messages

    # Worker environment
    os.environ["RAW_BUCKET"] = "raw-bucket-dev"
    os.environ["ENABLE_GZIP"] = "false"

    mod_wrk = _load_worker_module()
    # Patch shared ingestion service for s3 and yfinance
    import importlib

    svc = importlib.import_module("shared.ingestion.service")
    s3 = _S3Stub()
    monkeypatch.setitem(svc.__dict__, "boto3", _BotoStub(s3=s3))
    monkeypatch.setitem(svc.__dict__, "YahooFinanceClient", lambda: _YFStub(["AAPL", "MSFT", "GOOG"]))

    sqs_event = _sqs_records_from_batches(sqs.batches)
    wr = mod_wrk["main"](sqs_event, None)
    assert wr == {"batchItemFailures": []}
    # 3 symbols -> 3 puts
    assert len(s3.put_calls) == 3
    # Validate key structure
    for call in s3.put_calls:
        key = call["Key"]
        assert key.startswith("market/prices/ingestion_date=") and "/symbol=" in key


def test_e2e_gzip(monkeypatch):
    # Orchestrator prepares one symbol message
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["QUEUE_URL"] = "https://sqs.example/queue"
    os.environ["CHUNK_SIZE"] = "1"
    os.environ["SQS_SEND_BATCH_SIZE"] = "10"

    mod_orc = _load_orchestrator_module()
    sqs = _SQSStub()
    monkeypatch.setitem(mod_orc["main"].__globals__, "boto3", _BotoStub(sqs=sqs))

    event = {
        "symbols": ["AAPL"],
        "domain": "market",
        "table_name": "prices",
        "period": "1mo",
        "interval": "1d",
        "file_format": "json",
    }
    mod_orc["main"](event, None)

    # Worker with gzip enabled
    os.environ["RAW_BUCKET"] = "raw-bucket-dev"
    os.environ["ENABLE_GZIP"] = "true"

    mod_wrk = _load_worker_module()
    import importlib

    svc = importlib.import_module("shared.ingestion.service")
    s3 = _S3Stub()
    monkeypatch.setitem(svc.__dict__, "boto3", _BotoStub(s3=s3))
    monkeypatch.setitem(svc.__dict__, "YahooFinanceClient", lambda: _YFStub(["AAPL"]))

    sqs_event = _sqs_records_from_batches(sqs.batches)
    wr = mod_wrk["main"](sqs_event, None)
    assert wr == {"batchItemFailures": []}
    assert len(s3.put_calls) == 1
    call = s3.put_calls[0]
    assert call["ContentEncoding"] == "gzip"
    assert call["Key"].endswith(".gz")


def test_e2e_partial_batch_failure(monkeypatch):
    # Prepare two messages: one valid, one invalid JSON
    sqs_batches = [
        [
            {
                "Id": "m-0",
                "MessageBody": json.dumps(
                    {
                        "symbols": ["AAPL"],
                        "domain": "market",
                        "table_name": "prices",
                        "period": "1mo",
                        "interval": "1d",
                        "file_format": "json",
                    }
                ),
            },
            {"Id": "m-1", "MessageBody": "{not-json}"},
        ]
    ]

    os.environ["ENVIRONMENT"] = "dev"
    os.environ["RAW_BUCKET"] = "raw-bucket-dev"
    os.environ["ENABLE_GZIP"] = "false"

    mod_wrk = _load_worker_module()
    import importlib

    svc = importlib.import_module("shared.ingestion.service")
    s3 = _S3Stub()
    monkeypatch.setitem(svc.__dict__, "boto3", _BotoStub(s3=s3))
    monkeypatch.setitem(svc.__dict__, "YahooFinanceClient", lambda: _YFStub(["AAPL"]))

    sqs_event = _sqs_records_from_batches(sqs_batches)
    wr = mod_wrk["main"](sqs_event, None)
    assert wr["batchItemFailures"] and wr["batchItemFailures"][0]["itemIdentifier"] == "m-1"
    # Valid one still writes
    assert len(s3.put_calls) == 1


def test_e2e_idempotency_skip(monkeypatch):
    # Orchestrator for two symbols
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["QUEUE_URL"] = "https://sqs.example/queue"
    os.environ["CHUNK_SIZE"] = "2"
    os.environ["SQS_SEND_BATCH_SIZE"] = "10"

    mod_orc = _load_orchestrator_module()
    sqs = _SQSStub()
    monkeypatch.setitem(mod_orc["main"].__globals__, "boto3", _BotoStub(sqs=sqs))

    event = {
        "symbols": ["AAPL", "MSFT"],
        "domain": "market",
        "table_name": "prices",
        "period": "1mo",
        "interval": "1d",
        "file_format": "json",
    }
    mod_orc["main"](event, None)

    # Worker sees existing prefix for MSFT -> skip write for MSFT
    os.environ["RAW_BUCKET"] = "raw-bucket-dev"
    os.environ["ENABLE_GZIP"] = "false"
    mod_wrk = _load_worker_module()
    import importlib

    svc = importlib.import_module("shared.ingestion.service")
    s3 = _S3Stub(existing_symbol="MSFT")
    monkeypatch.setitem(svc.__dict__, "boto3", _BotoStub(s3=s3))
    monkeypatch.setitem(svc.__dict__, "YahooFinanceClient", lambda: _YFStub(["AAPL", "MSFT"]))

    sqs_event = _sqs_records_from_batches(sqs.batches)
    wr = mod_wrk["main"](sqs_event, None)
    assert wr == {"batchItemFailures": []}
    # Only AAPL written
    assert len(s3.put_calls) == 1
    assert "/symbol=AAPL/" in s3.put_calls[0]["Key"]
