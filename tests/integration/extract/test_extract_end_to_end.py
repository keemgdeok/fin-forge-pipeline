import os
import json
import runpy
from datetime import datetime, timezone

import boto3
from moto import mock_aws

# Ensure default region for moto/boto3 clients created without explicit region
os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")


def _load_orchestrator_module():
    return runpy.run_path("src/lambda/functions/ingestion_orchestrator/handler.py")


def _load_worker_module():
    return runpy.run_path("src/lambda/functions/ingestion_worker/handler.py")


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


def _receive_all_sqs_messages(queue_url: str):
    sqs = boto3.client("sqs", region_name=os.environ.get("AWS_REGION", "us-east-1"))
    records = []
    while True:
        resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=0)
        msgs = resp.get("Messages", [])
        if not msgs:
            break
        for m in msgs:
            records.append({"messageId": m["MessageId"], "body": m["Body"]})
    return {"Records": records}


@mock_aws
def test_e2e_basic_flow(monkeypatch):
    # Orchestrator environment
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["CHUNK_SIZE"] = "2"
    os.environ["SQS_SEND_BATCH_SIZE"] = "10"
    # moto SQS queue
    sqs = boto3.client("sqs", region_name="us-east-1")
    q = sqs.create_queue(QueueName="extract-e2e-queue")
    queue_url = q["QueueUrl"]
    os.environ["QUEUE_URL"] = queue_url
    # moto S3 bucket
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket = "raw-bucket-dev"
    s3.create_bucket(Bucket=bucket)

    event = {
        "symbols": ["AAPL", "MSFT", "GOOG"],
        "domain": "market",
        "table_name": "prices",
        "period": "1mo",
        "interval": "1d",
        "file_format": "json",
    }

    mod_orc = _load_orchestrator_module()
    resp = mod_orc["main"](event, None)
    assert resp["published"] == 2

    # Worker environment
    os.environ["RAW_BUCKET"] = bucket
    os.environ["ENABLE_GZIP"] = "false"
    mod_wrk = _load_worker_module()
    # Patch only YahooFinance (AWS to moto)
    import importlib

    svc = importlib.import_module("shared.ingestion.service")
    monkeypatch.setitem(svc.__dict__, "YahooFinanceClient", lambda: _YFStub(["AAPL", "MSFT", "GOOG"]))

    sqs_event = _receive_all_sqs_messages(queue_url)
    wr = mod_wrk["main"](sqs_event, None)
    assert wr == {"batchItemFailures": []}
    listed = s3.list_objects_v2(Bucket=bucket, Prefix="market/prices/")
    assert int(listed.get("KeyCount", 0)) >= 3


@mock_aws
def test_e2e_gzip(monkeypatch):
    # Orchestrator prepares one symbol message
    os.environ["ENVIRONMENT"] = "dev"
    sqs = boto3.client("sqs", region_name="us-east-1")
    q = sqs.create_queue(QueueName="extract-e2e-queue-gzip")
    queue_url = q["QueueUrl"]
    os.environ["QUEUE_URL"] = queue_url
    os.environ["CHUNK_SIZE"] = "1"
    os.environ["SQS_SEND_BATCH_SIZE"] = "10"

    mod_orc = _load_orchestrator_module()

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
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket = "raw-bucket-dev"
    s3.create_bucket(Bucket=bucket)
    os.environ["RAW_BUCKET"] = bucket
    os.environ["ENABLE_GZIP"] = "true"
    mod_wrk = _load_worker_module()
    import importlib

    svc = importlib.import_module("shared.ingestion.service")
    monkeypatch.setitem(svc.__dict__, "YahooFinanceClient", lambda: _YFStub(["AAPL"]))

    sqs_event = _receive_all_sqs_messages(queue_url)
    wr = mod_wrk["main"](sqs_event, None)
    assert wr == {"batchItemFailures": []}
    listed = s3.list_objects_v2(Bucket=bucket, Prefix="market/prices/")
    assert int(listed.get("KeyCount", 0)) == 1
    key = listed["Contents"][0]["Key"]
    assert key.endswith(".gz")
    head = s3.head_object(Bucket=bucket, Key=key)
    assert head.get("ContentEncoding") == "gzip"


@mock_aws
def test_e2e_partial_batch_failure(monkeypatch):
    # Create queue with valid and invalid message
    sqs = boto3.client("sqs", region_name="us-east-1")
    q = sqs.create_queue(QueueName="extract-e2e-partial")
    queue_url = q["QueueUrl"]
    valid_body = json.dumps(
        {
            "symbols": ["AAPL"],
            "domain": "market",
            "table_name": "prices",
            "period": "1mo",
            "interval": "1d",
            "file_format": "json",
        }
    )
    sqs.send_message(QueueUrl=queue_url, MessageBody=valid_body)
    sqs.send_message(QueueUrl=queue_url, MessageBody="{not-json}")

    os.environ["ENVIRONMENT"] = "dev"
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket = "raw-bucket-dev"
    s3.create_bucket(Bucket=bucket)
    os.environ["RAW_BUCKET"] = bucket
    os.environ["ENABLE_GZIP"] = "false"
    mod_wrk = _load_worker_module()
    import importlib

    svc = importlib.import_module("shared.ingestion.service")
    monkeypatch.setitem(svc.__dict__, "YahooFinanceClient", lambda: _YFStub(["AAPL"]))

    sqs_event = _receive_all_sqs_messages(queue_url)
    wr = mod_wrk["main"](sqs_event, None)
    assert any(f.get("itemIdentifier") for f in wr.get("batchItemFailures", []))
    listed = s3.list_objects_v2(Bucket=bucket, Prefix="market/prices/")
    assert int(listed.get("KeyCount", 0)) == 1


@mock_aws
def test_e2e_idempotency_skip(monkeypatch):
    # Orchestrator for two symbols
    os.environ["ENVIRONMENT"] = "dev"
    sqs = boto3.client("sqs", region_name="us-east-1")
    q = sqs.create_queue(QueueName="extract-e2e-idem")
    queue_url = q["QueueUrl"]
    os.environ["QUEUE_URL"] = queue_url
    os.environ["CHUNK_SIZE"] = "2"
    os.environ["SQS_SEND_BATCH_SIZE"] = "10"

    mod_orc = _load_orchestrator_module()

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
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket = "raw-bucket-dev"
    s3.create_bucket(Bucket=bucket)
    os.environ["RAW_BUCKET"] = bucket
    os.environ["ENABLE_GZIP"] = "false"
    # Pre-create an existing MSFT object under today's prefix
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    prefix = f"market/prices/ingestion_date={today}/data_source=yahoo_finance/symbol=MSFT/interval=1d/period=1mo/"
    s3.put_object(Bucket=bucket, Key=f"{prefix}existing.json", Body=b"x")

    mod_wrk = _load_worker_module()
    import importlib

    svc = importlib.import_module("shared.ingestion.service")
    monkeypatch.setitem(svc.__dict__, "YahooFinanceClient", lambda: _YFStub(["AAPL", "MSFT"]))

    sqs_event = _receive_all_sqs_messages(queue_url)
    wr = mod_wrk["main"](sqs_event, None)
    assert wr == {"batchItemFailures": []}
    listed_msft = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    assert int(listed_msft.get("KeyCount", 0)) == 1
    prefix_aapl = f"market/prices/ingestion_date={today}/data_source=yahoo_finance/symbol=AAPL/"
    listed_aapl = s3.list_objects_v2(Bucket=bucket, Prefix=prefix_aapl)
    assert int(listed_aapl.get("KeyCount", 0)) >= 1
