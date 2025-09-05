from typing import Any, Dict, List


class _StubS3Client:
    def __init__(self, keycount: int = 0):
        self.keycount = keycount
        self.put_calls: List[Dict[str, Any]] = []

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


"""
Note: Local record stubs and datetime imports were removed in favor of
shared yf_stub fixture which produces deterministic records.
"""


def test_ingestion_s3_write_json(env_dev, load_module, yf_stub, s3_stub):
    env_dev(raw_bucket="raw-bucket-dev")
    mod = load_module("src/lambda/functions/data_ingestion/handler.py")
    # Patch service deps
    yf_stub(["AAPL", "MSFT"])
    s3 = s3_stub(keycount=0)

    event: Dict[str, Any] = {
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
    assert len(s3.put_calls) >= 1
    assert s3.put_calls[0]["ContentType"] == "application/json"


def test_ingestion_idempotency_skips(env_dev, load_module, yf_stub, s3_stub):
    env_dev(raw_bucket="raw-bucket-dev")
    mod = load_module("src/lambda/functions/data_ingestion/handler.py")
    yf_stub(["AAPL"])  # one record
    s3 = s3_stub(keycount=1)  # prefix exists -> skip

    event: Dict[str, Any] = {
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
    assert len(s3.put_calls) == 0
