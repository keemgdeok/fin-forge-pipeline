import os
import runpy
from typing import Any, Dict
from botocore.exceptions import ClientError


def _load_orchestrator() -> Dict[str, Any]:
    return runpy.run_path("src/lambda/functions/ingestion_orchestrator/handler.py")


def test_orchestrator_fallback_to_default_symbol_when_no_sources(monkeypatch) -> None:
    """
    Given: SSM/S3 심볼 소스가 설정되지 않고 event.symbols도 비어있음
    When: 오케스트레이터 main을 실행하면
    Then: 기본값 심볼(AAPL)로 1개 청크가 발행되어야 함
    """
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["QUEUE_URL"] = "https://sqs.example/queue"
    os.environ.pop("SYMBOLS_SSM_PARAM", None)
    os.environ.pop("SYMBOLS_S3_BUCKET", None)
    os.environ.pop("SYMBOLS_S3_KEY", None)
    os.environ["CHUNK_SIZE"] = "10"
    os.environ["SQS_SEND_BATCH_SIZE"] = "10"

    mod = _load_orchestrator()

    sent_batches = []

    class _SQS:
        def send_message_batch(self, **kwargs):
            sent_batches.append(kwargs["Entries"])
            return {"Successful": [{"Id": e["Id"]} for e in kwargs["Entries"]]}

    class _Boto:
        def client(self, name: str):
            assert name == "sqs"
            return _SQS()

    # patch boto3 in main's globals
    monkeypatch.setitem(mod["main"].__globals__, "boto3", _Boto())

    event: Dict[str, Any] = {
        "symbols": [],
        "domain": "market",
        "table_name": "prices",
        "period": "1mo",
        "interval": "1d",
        "file_format": "json",
    }
    resp = mod["main"](event, None)
    assert resp["published"] == 1
    assert len(sent_batches) == 1
    assert len(sent_batches[0]) == 1


def test_orchestrator_ssm_error_then_s3_error_then_default(monkeypatch) -> None:
    """
    Given: SSM 파라미터 조회가 ClientError로 실패하고 S3 get_object도 실패
    When: 오케스트레이터 main을 실행하면
    Then: 기본 심볼(AAPL)로 1개 메시지가 발행되어야 함
    """
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["QUEUE_URL"] = "https://sqs.example/queue"
    os.environ["SYMBOLS_SSM_PARAM"] = "/ingestion/symbols"
    os.environ["SYMBOLS_S3_BUCKET"] = "cfg-bkt"
    os.environ["SYMBOLS_S3_KEY"] = "symbols.txt"

    mod = _load_orchestrator()

    class _SSM:
        def get_parameter(self, **kwargs):
            raise ClientError({"Error": {"Code": "AccessDenied", "Message": "nope"}}, "GetParameter")

    class _S3:
        def get_object(self, **kwargs):
            raise ClientError({"Error": {"Code": "NoSuchKey", "Message": "nope"}}, "GetObject")

    sent_batches = []

    class _SQS:
        def send_message_batch(self, **kwargs):
            sent_batches.append(kwargs["Entries"])
            return {"Successful": [{"Id": e["Id"]} for e in kwargs["Entries"]]}

    class _Boto:
        def client(self, name: str):
            if name == "ssm":
                return _SSM()
            if name == "s3":
                return _S3()
            if name == "sqs":
                return _SQS()
            raise AssertionError(name)

    monkeypatch.setitem(mod["main"].__globals__, "boto3", _Boto())

    event: Dict[str, Any] = {"symbols": []}
    resp = mod["main"](event, None)
    assert resp["published"] == 1
    assert len(sent_batches) == 1
    assert len(sent_batches[0]) == 1

