import os
import sys
from pathlib import Path
from typing import Any, Callable, Iterator
import pytest
import boto3

# Ensure 'shared' layer is importable at collection time (module import stage)
_repo_root = Path(__file__).resolve().parents[1]
_shared_path = _repo_root / "src" / "lambda" / "layers" / "common" / "python"
_shared_str = str(_shared_path)
if _shared_str not in sys.path:
    sys.path.insert(0, _shared_str)


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Ensure default AWS region is set for moto/boto3 clients."""
    monkeypatch.setenv("AWS_REGION", os.environ.get("AWS_REGION", "us-east-1"))
    monkeypatch.setenv("AWS_DEFAULT_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
    # Provide dummy credentials so botocore signing doesn't fail under moto
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", os.environ.get("AWS_ACCESS_KEY_ID", "testing"))
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", os.environ.get("AWS_SECRET_ACCESS_KEY", "testing"))
    monkeypatch.setenv("AWS_SESSION_TOKEN", os.environ.get("AWS_SESSION_TOKEN", "testing"))
    yield


@pytest.fixture(autouse=True)
def pythonpath() -> Iterator[None]:
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
def orchestrator_env(monkeypatch: pytest.MonkeyPatch) -> Callable[[str], None]:
    """Apply orchestrator-related environment variables."""

    def _apply(queue_url: str, *, chunk_size: int = 2, batch_size: int = 10, environment: str = "dev") -> None:
        monkeypatch.setenv("QUEUE_URL", queue_url)
        monkeypatch.setenv("CHUNK_SIZE", str(chunk_size))
        monkeypatch.setenv("SQS_SEND_BATCH_SIZE", str(batch_size))
        monkeypatch.setenv("ENVIRONMENT", environment)

    return _apply


@pytest.fixture
def worker_env(monkeypatch: pytest.MonkeyPatch) -> Callable[[str], None]:
    """Apply worker-related environment variables."""

    def _apply(raw_bucket: str, *, enable_gzip: bool = False, environment: str = "dev") -> None:
        monkeypatch.setenv("RAW_BUCKET", raw_bucket)
        monkeypatch.setenv("ENABLE_GZIP", "true" if enable_gzip else "false")
        monkeypatch.setenv("ENVIRONMENT", environment)

    return _apply


@pytest.fixture
def yf_stub(monkeypatch: pytest.MonkeyPatch) -> Callable[[list[str]], None]:
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
def s3_stub(monkeypatch: pytest.MonkeyPatch) -> Callable[..., Any]:
    """Patch boto3 in shared.ingestion.service with a lightweight S3 stub.

    Returns the stub instance which captures put_calls for assertions and
    responds to list_objects_v2 with a configurable KeyCount.
    """
    import importlib
    from tests.fixtures.clients import S3Stub, BotoStub

    def _apply(*, keycount: int = 0) -> Any:
        svc = importlib.import_module("shared.ingestion.service")
        s3 = S3Stub(keycount=keycount)
        monkeypatch.setitem(svc.__dict__, "boto3", BotoStub(s3=s3))
        return s3

    return _apply


@pytest.fixture
def env_dev(monkeypatch: pytest.MonkeyPatch) -> Callable[..., None]:
    """Set ENVIRONMENT=dev and optionally RAW_BUCKET.

    Usage: env_dev() or env_dev(raw_bucket="raw-bucket-dev").
    """

    def _apply(*, raw_bucket: str | None = None) -> None:
        monkeypatch.setenv("ENVIRONMENT", "dev")
        if raw_bucket:
            monkeypatch.setenv("RAW_BUCKET", raw_bucket)

    return _apply


@pytest.fixture
def load_module() -> Callable[[str], dict[str, Any]]:
    import runpy

    def _apply(path: str) -> dict[str, Any]:
        return runpy.run_path(path)

    return _apply


@pytest.fixture
def fake_python_function(monkeypatch: pytest.MonkeyPatch) -> Callable[[Any], None]:
    from aws_cdk import Duration

    def _apply(target_module: Any) -> None:
        from aws_cdk import aws_lambda as lambda_

        def _fake(scope, id, **kwargs):
            return lambda_.Function(
                scope,
                id,
                runtime=lambda_.Runtime.PYTHON_3_12,
                handler="index.handler",
                code=lambda_.Code.from_inline("def handler(event, context): return {}"),
                memory_size=kwargs.get("memory_size", 128),
                timeout=kwargs.get("timeout", Duration.seconds(10)),
                log_retention=kwargs.get("log_retention"),
                role=kwargs.get("role"),
                layers=kwargs.get("layers", []),
                environment=kwargs.get("environment", {}),
                reserved_concurrent_executions=kwargs.get("reserved_concurrent_executions"),
            )

        monkeypatch.setattr(target_module, "PythonFunction", _fake, raising=False)

    return _apply


@pytest.fixture
def make_queue() -> Callable[[str], str]:
    def _create(name: str) -> str:
        client = boto3.client("sqs", region_name=os.environ.get("AWS_REGION", "us-east-1"))
        return client.create_queue(QueueName=name)["QueueUrl"]

    return _create


@pytest.fixture
def make_bucket() -> Callable[[str], str]:
    def _create(name: str) -> str:
        client = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))
        client.create_bucket(Bucket=name)
        return name

    return _create
