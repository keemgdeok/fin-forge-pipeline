from typing import Any, Dict


def test_ingestion_symbols_parsing(env_dev, load_module):
    env_dev(raw_bucket="raw-bucket-dev")

    main = load_module("src/lambda/functions/data_ingestion/handler.py")["main"]
    event = {
        "data_source": "yahoo_finance",
        "data_type": "prices",
        "symbols": ["AAPL", " ", 123, "MSFT"],
        "period": "1y",
        "interval": "1d",
        "domain": "market",
        "table_name": "prices",
        "file_format": "parquet",
    }

    resp = main(event, None)
    assert resp["statusCode"] == 200
    body = resp["body"]
    assert body["symbols_processed"] == ["AAPL", "MSFT"]
    assert "invalid_symbols" in body


def test_ingestion_does_not_call_stepfunctions(env_dev, load_module, yf_stub, s3_stub):
    env_dev(raw_bucket="raw-bucket-dev")
    main = load_module("src/lambda/functions/data_ingestion/handler.py")["main"]

    # Patch YahooFinanceClient to return one record and S3 client stub
    yf_stub(["AAPL"])
    s3_stub(keycount=0)

    event: Dict[str, Any] = {
        "data_source": "yahoo_finance",
        "data_type": "prices",
        "symbols": ["AAPL"],
        "period": "1mo",
        "interval": "1d",
        "domain": "market",
        "table_name": "prices",
        "file_format": "json",
    }

    resp = main(event, None)
    assert resp["statusCode"] == 200
    assert resp["body"]["processed_records"] == 1
