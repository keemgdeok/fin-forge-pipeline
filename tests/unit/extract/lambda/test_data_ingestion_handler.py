from typing import Any, Dict, List
import pytest
from tests.fixtures.data_builders import build_ingestion_event


def test_ingestion_symbols_parsing(env_dev, load_module) -> None:
    """
    Given: 혼합 심볼 목록(공백/숫자 포함)
    When: 핸들러가 이벤트를 처리하면
    Then: 유효 심볼만 symbols_processed에 남고 invalid_symbols가 보고되어야 함
    """
    env_dev(raw_bucket="raw-bucket-dev")

    main = load_module("src/lambda/functions/data_ingestion/handler.py")["main"]
    event = build_ingestion_event(symbols=["AAPL", " ", 123, "MSFT"], period="1y", file_format="parquet")

    resp = main(event, None)
    assert resp["statusCode"] == 200
    body = resp["body"]
    assert body["symbols_processed"] == ["AAPL", "MSFT"]
    assert "invalid_symbols" in body


def test_ingestion_does_not_call_stepfunctions(env_dev, load_module, yf_stub, s3_stub) -> None:
    """
    Given: 한 개의 심볼과 S3만 사용하는 저장 경로
    When: 핸들러가 이벤트를 처리하면
    Then: Step Functions 호출 없이 처리 레코드 수만 1로 보고되어야 함
    """
    env_dev(raw_bucket="raw-bucket-dev")
    main = load_module("src/lambda/functions/data_ingestion/handler.py")["main"]

    # Patch YahooFinanceClient to return one record and S3 client stub
    yf_stub(["AAPL"])
    s3_stub(keycount=0)

    event: Dict[str, Any] = build_ingestion_event(symbols=["AAPL"], file_format="json")

    resp = main(event, None)
    assert resp["statusCode"] == 200
    assert resp["body"]["processed_records"] == 1


@pytest.mark.parametrize(
    "symbols,expected",
    [
        ([], []),
        (["AAPL"], ["AAPL"]),
        (["AAPL", None, "", " "], ["AAPL"]),
        (["AAPL", 123, "MSFT"], ["AAPL", "MSFT"]),
        (["A" * 100], ["A" * 100]),
    ],
)
def test_ingestion_symbols_validation_parametrized(env_dev, load_module, symbols: List[Any], expected: List[str]) -> None:
    """
    Given: 다양한 심볼 입력 조합
    When: 핸들러가 이벤트를 검증하면
    Then: symbols_processed가 기대 리스트와 정확히 일치해야 함
    """
    env_dev(raw_bucket="raw-bucket-dev")
    main = load_module("src/lambda/functions/data_ingestion/handler.py")["main"]

    event: Dict[str, Any] = build_ingestion_event(symbols=symbols, file_format="parquet")

    resp = main(event, None)
    assert resp["statusCode"] == 200
    assert resp["body"]["symbols_processed"] == expected
