import os
import runpy


def load_ingestion_main():
    mod = runpy.run_path("src/lambda/functions/data_ingestion/handler.py")
    return mod["main"]


def test_ingestion_symbols_parsing():
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["RAW_BUCKET"] = "raw-bucket-dev"

    main = load_ingestion_main()
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
