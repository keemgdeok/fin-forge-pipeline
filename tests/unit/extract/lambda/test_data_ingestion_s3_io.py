from typing import Any, Dict, List


"""
Note: Local record stubs and datetime imports were removed in favor of
shared yf_stub fixture which produces deterministic records.
"""


def test_ingestion_s3_write_json(env_dev, load_module, yf_stub, s3_stub):
    """
    Given: 두 심볼(AAPL, MSFT)과 RAW 버킷이 존재
    When: JSON 포맷으로 인제스트하면
    Then: 처리 레코드 2, 최소 1개 이상의 S3 put, ContentType이 application/json이어야 함
    """
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
    """
    Given: 대상 prefix에 기존 객체가 존재(keycount=1)
    When: 동일 심볼을 인제스트하면
    Then: idempotency로 인해 쓰기가 생략되고 put 호출이 없어야 함
    """
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
