import os
import sys
from pathlib import Path
import pytest
import boto3


@pytest.fixture(autouse=True)
def aws_env(monkeypatch):
    """Ensure default AWS region is set for moto/boto3 clients."""
    monkeypatch.setenv("AWS_REGION", os.environ.get("AWS_REGION", "us-east-1"))
    monkeypatch.setenv("AWS_DEFAULT_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
    # Provide dummy credentials so botocore signing doesn't fail under moto
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", os.environ.get("AWS_ACCESS_KEY_ID", "testing"))
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", os.environ.get("AWS_SECRET_ACCESS_KEY", "testing"))
    monkeypatch.setenv("AWS_SESSION_TOKEN", os.environ.get("AWS_SESSION_TOKEN", "testing"))
    yield


@pytest.fixture(autouse=True)
def pythonpath():
    """Ensure Lambda layer 'shared' package is importable in tests.

    Adds src/lambda/layers/common/python to sys.path so that `import shared.*`
    used by Lambda handlers works when loading via runpy.
    """
    repo_root = Path(__file__).resolve().parents[1]
    shared_path = repo_root / "src" / "lambda" / "layers" / "common" / "python"
    shared_str = str(shared_path)
    if shared_str not in sys.path:
        sys.path.insert(0, shared_str)
    yield


@pytest.fixture
def orchestrator_env(monkeypatch):
    """Apply orchestrator-related environment variables."""

    def _apply(queue_url: str, *, chunk_size: int = 2, batch_size: int = 10, environment: str = "dev") -> None:
        monkeypatch.setenv("QUEUE_URL", queue_url)
        monkeypatch.setenv("CHUNK_SIZE", str(chunk_size))
        monkeypatch.setenv("SQS_SEND_BATCH_SIZE", str(batch_size))
        monkeypatch.setenv("ENVIRONMENT", environment)

    return _apply


@pytest.fixture
def worker_env(monkeypatch):
    """Apply worker-related environment variables."""

    def _apply(raw_bucket: str, *, enable_gzip: bool = False, environment: str = "dev") -> None:
        monkeypatch.setenv("RAW_BUCKET", raw_bucket)
        monkeypatch.setenv("ENABLE_GZIP", "true" if enable_gzip else "false")
        monkeypatch.setenv("ENVIRONMENT", environment)

    return _apply


@pytest.fixture
def yf_stub(monkeypatch):
    """Patch YahooFinanceClient with a deterministic stub returning fixed records."""
    import importlib

    def _apply(symbols: list[str]):
        svc = importlib.import_module("shared.ingestion.service")

        class _Rec:
            def __init__(self, symbol: str):
                from datetime import datetime, timezone

                self.symbol = symbol
                self.timestamp = datetime(2024, 1, 1, tzinfo=timezone.utc)
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
            def fetch_prices(self, _symbols, period, interval):
                return [_Rec(s) for s in symbols]

        monkeypatch.setitem(svc.__dict__, "YahooFinanceClient", lambda: _YFStub())

    return _apply


@pytest.fixture
def make_queue():
    def _create(name: str) -> str:
        client = boto3.client("sqs", region_name=os.environ.get("AWS_REGION", "us-east-1"))
        return client.create_queue(QueueName=name)["QueueUrl"]

    return _create


@pytest.fixture
def make_bucket():
    def _create(name: str) -> str:
        client = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))
        client.create_bucket(Bucket=name)
        return name

    return _create
