from __future__ import annotations

import json
from typing import Any, Dict


def test_worker_returns_failure_on_malformed_json(load_module) -> None:
    """
    Given: 본문이 깨진 JSON인 SQS 레코드 1건
    When: 워커 핸들러 실행
    Then: 해당 메시지 ID만 batchItemFailures에 포함
    """
    mod = load_module("src/lambda/functions/ingestion_worker/handler.py")
    sqs_event: Dict[str, Any] = {"Records": [{"messageId": "m1", "body": "{not-json}"}]}
    wr = mod["main"](sqs_event, None)
    assert wr["batchItemFailures"] == [{"itemIdentifier": "m1"}]


def test_worker_reports_partial_failures_for_mixed_batch(monkeypatch, load_module) -> None:
    """
    Given: 정상/비정상 메시지가 섞인 배치(1 정상, 1 깨진 JSON)
    When: 워커 핸들러 실행
    Then: 깨진 JSON 메시지만 실패로 보고
    """
    mod = load_module("src/lambda/functions/ingestion_worker/handler.py")

    def _ok(_payload: Dict[str, Any], _ctx: Any) -> Dict[str, Any]:
        return {}

    # process_event를 정상 스텁으로 패치
    monkeypatch.setitem(mod["main"].__globals__, "process_event", _ok)

    valid_body = json.dumps({"symbols": ["AAPL"]})
    sqs_event: Dict[str, Any] = {
        "Records": [
            {"messageId": "v1", "body": valid_body},
            {"messageId": "b1", "body": "{not-json}"},
        ]
    }
    wr = mod["main"](sqs_event, None)
    assert wr["batchItemFailures"] == [{"itemIdentifier": "b1"}]


def test_worker_marks_failure_when_process_event_raises(monkeypatch, load_module) -> None:
    """
    Given: 처리 로직이 예외를 던지는 유효 JSON 메시지
    When: 워커 핸들러 실행
    Then: 해당 메시지 ID가 실패로 보고
    """
    mod = load_module("src/lambda/functions/ingestion_worker/handler.py")

    def _boom(_payload: Dict[str, Any], _ctx: Any) -> Dict[str, Any]:
        raise Exception("forced")

    monkeypatch.setitem(mod["main"].__globals__, "process_event", _boom)

    sqs_event: Dict[str, Any] = {"Records": [{"messageId": "m2", "body": json.dumps({"symbols": ["MSFT"]})}]}
    wr = mod["main"](sqs_event, None)
    assert wr["batchItemFailures"] == [{"itemIdentifier": "m2"}]


def test_worker_success_when_all_valid(monkeypatch, load_module) -> None:
    """
    Given: 모두 유효한 JSON 메시지 두 건
    When: 워커 핸들러 실행
    Then: 실패 목록은 비어 있음
    """
    mod = load_module("src/lambda/functions/ingestion_worker/handler.py")

    def _ok(_payload: Dict[str, Any], _ctx: Any) -> Dict[str, Any]:
        return {}

    monkeypatch.setitem(mod["main"].__globals__, "process_event", _ok)

    sqs_event: Dict[str, Any] = {
        "Records": [
            {"messageId": "m1", "body": json.dumps({"symbols": ["AAPL"]})},
            {"messageId": "m2", "body": json.dumps({"symbols": ["GOOG"]})},
        ]
    }
    wr = mod["main"](sqs_event, None)
    assert wr["batchItemFailures"] == []


def test_worker_marks_failure_when_process_event_returns_500(monkeypatch, load_module) -> None:
    """
    Given: process_event가 예외 없이 500 응답을 반환
    When: 워커 핸들러 실행
    Then: 해당 메시지 ID가 실패로 보고
    """
    mod = load_module("src/lambda/functions/ingestion_worker/handler.py")

    def _fail_ret(_payload: Dict[str, Any], _ctx: Any) -> Dict[str, Any]:
        return {"statusCode": 500, "body": {"error": "x"}}

    monkeypatch.setitem(mod["main"].__globals__, "process_event", _fail_ret)

    sqs_event: Dict[str, Any] = {"Records": [{"messageId": "m3", "body": json.dumps({"symbols": ["AAPL"]})}]}
    wr = mod["main"](sqs_event, None)
    assert wr["batchItemFailures"] == [{"itemIdentifier": "m3"}]


def test_worker_marks_failure_on_timeout_error(monkeypatch, load_module) -> None:
    """
    Given: process_event가 TimeoutError를 발생
    When: 워커 핸들러 실행
    Then: 해당 메시지 ID가 실패로 보고
    """
    mod = load_module("src/lambda/functions/ingestion_worker/handler.py")

    def _timeout(_payload: Dict[str, Any], _ctx: Any) -> Dict[str, Any]:
        raise TimeoutError("t")

    monkeypatch.setitem(mod["main"].__globals__, "process_event", _timeout)

    sqs_event: Dict[str, Any] = {"Records": [{"messageId": "m4", "body": json.dumps({"symbols": ["AAPL"]})}]}
    wr = mod["main"](sqs_event, None)
    assert wr["batchItemFailures"] == [{"itemIdentifier": "m4"}]


def test_worker_marks_failure_on_memory_error(monkeypatch, load_module) -> None:
    """
    Given: process_event가 MemoryError를 발생
    When: 워커 핸들러 실행
    Then: 해당 메시지 ID가 실패로 보고
    """
    mod = load_module("src/lambda/functions/ingestion_worker/handler.py")

    def _mem(_payload: Dict[str, Any], _ctx: Any) -> Dict[str, Any]:
        raise MemoryError("oom")

    monkeypatch.setitem(mod["main"].__globals__, "process_event", _mem)

    sqs_event: Dict[str, Any] = {"Records": [{"messageId": "m5", "body": json.dumps({"symbols": ["AAPL"]})}]}
    wr = mod["main"](sqs_event, None)
    assert wr["batchItemFailures"] == [{"itemIdentifier": "m5"}]


def test_worker_marks_failure_on_client_error(monkeypatch, load_module) -> None:
    """
    Given: process_event가 ClientError(예: S3 put 실패)를 발생
    When: 워커 핸들러 실행
    Then: 해당 메시지 ID가 실패로 보고
    """
    from botocore.exceptions import ClientError

    mod = load_module("src/lambda/functions/ingestion_worker/handler.py")

    def _client_error(_payload: Dict[str, Any], _ctx: Any) -> Dict[str, Any]:
        raise ClientError({"Error": {"Code": "500", "Message": "S3Fail"}}, "PutObject")

    monkeypatch.setitem(mod["main"].__globals__, "process_event", _client_error)

    sqs_event: Dict[str, Any] = {"Records": [{"messageId": "m6", "body": json.dumps({"symbols": ["AAPL"]})}]}
    wr = mod["main"](sqs_event, None)
    assert wr["batchItemFailures"] == [{"itemIdentifier": "m6"}]
