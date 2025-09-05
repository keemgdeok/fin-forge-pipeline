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


def test_ingestion_does_not_call_stepfunctions(monkeypatch):
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["RAW_BUCKET"] = "raw-bucket-dev"
    # 과거 코드 회귀 방지: 환경변수가 있어도 SFN을 호출하지 않아야 함
    os.environ["STEP_FUNCTION_ARN"] = "arn:aws:states:us-east-1:111111111111:stateMachine:dev-processing"

    mod = runpy.run_path("src/lambda/functions/data_ingestion/handler.py")
    main = mod["main"]

    # Stub YahooFinanceClient to return at least 1 record
    class _Rec:
        symbol = "AAPL"
        from datetime import datetime, timezone

        timestamp = datetime(2024, 1, 1, tzinfo=timezone.utc)
        open = 1.0
        high = 1.0
        low = 1.0
        close = 1.0
        volume = 1.0

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

    class _YF:
        def fetch_prices(self, symbols, period, interval):
            return [_Rec()]

    # Stub boto3: s3는 허용, stepfunctions는 호출되면 실패
    class _S3:
        def list_objects_v2(self, **kwargs):
            return {"KeyCount": 0}

        def put_object(self, **kwargs):
            return {"ETag": "test"}

    class _Boto:
        @staticmethod
        def client(name):
            if name == "s3":
                return _S3()
            if name == "stepfunctions":
                raise AssertionError("stepfunctions client must not be created")
            raise NotImplementedError(name)

    # Patch shared service module used by handler
    import importlib

    svc = importlib.import_module("shared.ingestion.service")
    monkeypatch.setitem(svc.__dict__, "YahooFinanceClient", _YF)
    monkeypatch.setitem(svc.__dict__, "boto3", _Boto)

    event = {
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
