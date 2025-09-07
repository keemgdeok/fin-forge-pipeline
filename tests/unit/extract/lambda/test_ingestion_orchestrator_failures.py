from __future__ import annotations

import os
from typing import Any

import runpy
import pytest
from botocore.exceptions import ClientError


def _load_orchestrator():
    return runpy.run_path("src/lambda/functions/ingestion_orchestrator/handler.py")


def test_orchestrator_raises_on_sqs_client_error(monkeypatch) -> None:
    """
    Given: send_message_batch가 ClientError를 던지는 SQS 스텁
    When: 오케스트레이터 main 실행
    Then: 예외가 전파되어 테스트에서 포착됨
    """
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["QUEUE_URL"] = "https://sqs.example/queue"
    os.environ["CHUNK_SIZE"] = "2"
    os.environ["SQS_SEND_BATCH_SIZE"] = "10"

    mod = _load_orchestrator()

    class _SQS:
        def send_message_batch(self, **kwargs: Any):
            raise ClientError({"Error": {"Code": "500", "Message": "boom"}}, "SendMessageBatch")

    class _Boto:
        def client(self, name: str, **kwargs: Any):
            assert name == "sqs"
            return _SQS()

    # Patch boto3 in main's globals
    monkeypatch.setitem(mod["main"].__globals__, "boto3", _Boto())

    event = {
        "symbols": ["AAPL", "MSFT"],
        "domain": "market",
        "table_name": "prices",
        "period": "1mo",
        "interval": "1d",
        "file_format": "json",
    }

    with pytest.raises(ClientError):
        mod["main"](event, None)


def test_orchestrator_retries_partial_failure_then_succeeds(monkeypatch) -> None:
    """
    Given: 첫 배치 응답에 Failed 1건, 재시도 시 성공
    When: 오케스트레이터 main 실행
    Then: 예외 없이 완료되고 published가 전송된 메시지 수(청크 수)와 동일(=1)
    """
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["QUEUE_URL"] = "https://sqs.example/queue"
    os.environ["CHUNK_SIZE"] = "3"  # 한 메시지(청크)로 3심볼을 담음 → 메시지 수 1
    os.environ["SQS_SEND_BATCH_SIZE"] = "10"

    mod = _load_orchestrator()

    class _SQS:
        def __init__(self) -> None:
            self.calls: list[list[dict[str, Any]]] = []

        def send_message_batch(self, **kwargs: Any):
            entries = kwargs.get("Entries", [])
            self.calls.append(entries)
            # 1차 호출: 마지막 엔트리만 실패
            if len(self.calls) == 1:
                succ = [{"Id": e["Id"]} for e in entries[:-1]]
                fail = [{"Id": entries[-1]["Id"], "Code": "500", "Message": "boom"}]
                return {"Successful": succ, "Failed": fail}
            # 2차 호출(실패분만 재시도): 모두 성공 처리
            return {"Successful": [{"Id": e["Id"]} for e in entries], "Failed": []}

    class _Boto:
        def __init__(self) -> None:
            self._sqs = _SQS()

        def client(self, name: str, **kwargs: Any):
            assert name == "sqs"
            return self._sqs

    boto = _Boto()
    monkeypatch.setitem(mod["main"].__globals__, "boto3", boto)  # type: ignore[arg-type]

    event = {
        "symbols": ["AAPL", "MSFT", "GOOG"],
        "domain": "market",
        "table_name": "prices",
        "period": "1mo",
        "interval": "1d",
        "file_format": "json",
    }
    resp = mod["main"](event, None)
    # 청크 1개만 발행되므로 published=1/chunks=1 이어야 함
    assert resp["published"] == 1
    assert resp["chunks"] == 1
    # 첫 호출 실패→두 번째 호출 재시도까지 총 2회 호출되었는지 확인
    assert len(boto._sqs.calls) == 2


def test_orchestrator_retries_partial_failure_then_raises(monkeypatch) -> None:
    """
    Given: 첫 응답 Failed 1건, 재시도에서도 Failed 지속
    When: 오케스트레이터 main 실행
    Then: RuntimeError가 전파되어야 함
    """
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["QUEUE_URL"] = "https://sqs.example/queue"
    os.environ["CHUNK_SIZE"] = "3"
    os.environ["SQS_SEND_BATCH_SIZE"] = "10"

    mod = _load_orchestrator()

    class _SQS:
        def __init__(self) -> None:
            self.calls: list[list[dict[str, Any]]] = []

        def send_message_batch(self, **kwargs: Any):
            entries = kwargs.get("Entries", [])
            self.calls.append(entries)
            # 1차 호출: 마지막 엔트리 실패
            if len(self.calls) == 1:
                succ = [{"Id": e["Id"]} for e in entries[:-1]]
                fail = [{"Id": entries[-1]["Id"], "Code": "500", "Message": "boom"}]
                return {"Successful": succ, "Failed": fail}
            # 2차 호출: 여전히 실패
            return {
                "Successful": [],
                "Failed": [{"Id": entries[0]["Id"], "Code": "500", "Message": "still"}],
            }

    class _Boto:
        def __init__(self) -> None:
            self._sqs = _SQS()

        def client(self, name: str, **kwargs: Any):
            assert name == "sqs"
            return self._sqs

    boto = _Boto()
    monkeypatch.setitem(mod["main"].__globals__, "boto3", boto)  # type: ignore[arg-type]

    event = {
        "symbols": ["AAPL", "MSFT", "GOOG"],
        "domain": "market",
        "table_name": "prices",
        "period": "1mo",
        "interval": "1d",
        "file_format": "json",
    }
    import pytest

    with pytest.raises(RuntimeError):
        mod["main"](event, None)
