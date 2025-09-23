import os
import sys
from pathlib import Path
from typing import Any, Callable, Iterator, Optional
import pytest
import boto3


pytest_plugins = [
    "tests.fixtures.load_env",
    "tests.fixtures.load_agent",
]

# Ensure 'shared' layer is importable at collection time (module import stage)
_repo_root = Path(__file__).resolve().parents[1]
# Ensure project root and 'src' are on sys.path for flexible imports
_repo_root_str = str(_repo_root)
_src_path_str = str(_repo_root / "src")
if _repo_root_str not in sys.path:
    sys.path.insert(0, _repo_root_str)
if _src_path_str not in sys.path:
    sys.path.insert(0, _src_path_str)
_shared_path = _repo_root / "src" / "lambda" / "layers" / "common" / "python"
_shared_str = str(_shared_path)
if _shared_str not in sys.path:
    sys.path.insert(0, _shared_str)


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Ensure default AWS region is set for moto/boto3 clients and clear cross-test env leaks.

    Also removes external symbol source envs to prevent test cross contamination
    between integration and unit tests.
    """
    monkeypatch.setenv("AWS_REGION", os.environ.get("AWS_REGION", "us-east-1"))
    monkeypatch.setenv("AWS_DEFAULT_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
    # Provide dummy credentials so botocore signing doesn't fail under moto
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", os.environ.get("AWS_ACCESS_KEY_ID", "testing"))
    monkeypatch.setenv(
        "AWS_SECRET_ACCESS_KEY",
        os.environ.get("AWS_SECRET_ACCESS_KEY", "testing"),
    )
    monkeypatch.setenv("AWS_SESSION_TOKEN", os.environ.get("AWS_SESSION_TOKEN", "testing"))

    # Clear external symbol sources to avoid test cross-contamination
    monkeypatch.delenv("SYMBOLS_SSM_PARAM", raising=False)
    monkeypatch.delenv("SYMBOLS_S3_BUCKET", raising=False)
    monkeypatch.delenv("SYMBOLS_S3_KEY", raising=False)
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

    def _apply(
        queue_url: str,
        *,
        chunk_size: int = 2,
        batch_size: int = 10,
        environment: str = "dev",
    ) -> None:
        monkeypatch.setenv("QUEUE_URL", queue_url)
        monkeypatch.setenv("CHUNK_SIZE", str(chunk_size))
        monkeypatch.setenv("SQS_SEND_BATCH_SIZE", str(batch_size))
        monkeypatch.setenv("ENVIRONMENT", environment)

    return _apply


@pytest.fixture
def worker_env(monkeypatch: pytest.MonkeyPatch) -> Callable[[str], None]:
    """Apply worker-related environment variables."""

    def _apply(
        raw_bucket: str,
        *,
        enable_gzip: bool = False,
        environment: str = "dev",
        manifest_basename: str = "_batch",
        manifest_suffix: str = ".manifest.json",
    ) -> None:
        monkeypatch.setenv("RAW_BUCKET", raw_bucket)
        monkeypatch.setenv("ENABLE_GZIP", "true" if enable_gzip else "false")
        monkeypatch.setenv("ENVIRONMENT", environment)
        monkeypatch.setenv("RAW_MANIFEST_BASENAME", manifest_basename)
        monkeypatch.setenv("RAW_MANIFEST_SUFFIX", manifest_suffix)

    return _apply


# Removed yf_stub - not used in transform tests


@pytest.fixture
def s3_stub(monkeypatch):
    """Mock S3 client for unit tests using S3Stub class."""

    def _create_s3_stub(keycount=0, head_ok=True):
        from tests.fixtures.clients import S3Stub, BotoStub

        s3_mock = S3Stub(keycount=keycount, head_ok=head_ok)
        boto_mock = BotoStub(s3=s3_mock)
        monkeypatch.setattr("boto3.client", lambda service, **kwargs: boto_mock.client(service))
        return s3_mock

    return _create_s3_stub


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
def fake_python_function(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[Any], None]:
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


@pytest.fixture
def transform_env(monkeypatch: pytest.MonkeyPatch) -> Callable[..., None]:
    """Set transform pipeline environment variables.

    Usage: transform_env() for defaults or transform_env(domain="market", table_name="prices").
    """

    def _apply(
        *,
        environment: str = "test",
        domain: str = "market",
        table_name: str = "prices",
        raw_bucket: Optional[str] = None,
        curated_bucket: Optional[str] = None,
        artifacts_bucket: Optional[str] = None,
        max_backfill_days: int = 31,
    ) -> None:
        monkeypatch.setenv("ENVIRONMENT", environment)
        monkeypatch.setenv(
            "RAW_BUCKET",
            raw_bucket or f"data-pipeline-raw-{environment}-123456789012",
        )
        monkeypatch.setenv(
            "CURATED_BUCKET",
            curated_bucket or f"data-pipeline-curated-{environment}-123456789012",
        )
        monkeypatch.setenv(
            "ARTIFACTS_BUCKET",
            artifacts_bucket or f"data-pipeline-artifacts-{environment}-123456789012",
        )
        monkeypatch.setenv("MAX_BACKFILL_DAYS", str(max_backfill_days))
        monkeypatch.setenv("CATALOG_UPDATE_DEFAULT", "on_schema_change")

    return _apply


# Removed individual module fixtures - use load_module() fixture instead


def pytest_configure(config):
    """Configure pytest with essential markers."""
    # Essential markers only
    config.addinivalue_line("markers", "unit: unit test")
    config.addinivalue_line("markers", "integration: integration test")
    config.addinivalue_line("markers", "slow: slow running test")
    config.addinivalue_line("markers", "transform: transform pipeline test")


def pytest_addoption(parser):
    """Add essential command line options."""
    parser.addoption("--runslow", action="store_true", default=False, help="run slow tests")
    parser.addoption(
        "--transform-only",
        action="store_true",
        default=False,
        help="run only transform pipeline tests",
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers and filtering."""
    rootdir = Path(config.rootdir)

    for item in items:
        rel_path = Path(item.fspath).relative_to(rootdir)

        # Add markers based on file location
        if "unit" in rel_path.parts:
            item.add_marker(pytest.mark.unit)
        if "integration" in rel_path.parts:
            item.add_marker(pytest.mark.integration)
        if "transform" in rel_path.parts:
            item.add_marker(pytest.mark.transform)
        if "lambda" in rel_path.parts:
            item.add_marker(pytest.mark.lambda_test)
        if "glue" in rel_path.parts:
            item.add_marker(pytest.mark.glue)
        if "infrastructure" in rel_path.parts:
            item.add_marker(pytest.mark.infrastructure)

        # Add specific Lambda function markers
        if "preflight" in str(rel_path):
            item.add_marker(pytest.mark.preflight)
        if "schema_check" in str(rel_path):
            item.add_marker(pytest.mark.schema_check)
        if "build_dates" in str(rel_path):
            item.add_marker(pytest.mark.build_dates)


def pytest_runtest_setup(item):
    """Setup individual test runs with filtering."""
    # Skip slow tests unless --runslow is given
    if "slow" in item.keywords and not item.config.getoption("--runslow"):
        pytest.skip("need --runslow option to run")


@pytest.fixture
def yf_stub(monkeypatch):
    """Mock YahooFinanceClient for integration tests."""

    def _stub_yahoo_finance(symbols):
        from datetime import datetime, timezone
        from dataclasses import dataclass
        from typing import Optional

        @dataclass
        class MockPriceRecord:
            symbol: str
            timestamp: datetime
            open: Optional[float] = None
            high: Optional[float] = None
            low: Optional[float] = None
            close: Optional[float] = None
            volume: Optional[float] = None

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

        def mock_fetch_prices(self, symbols, period, interval):
            return [
                MockPriceRecord(
                    symbol=sym,
                    timestamp=datetime.now(timezone.utc),
                    open=100.0 + hash(sym) % 50,
                    high=105.0 + hash(sym) % 50,
                    low=95.0 + hash(sym) % 50,
                    close=102.0 + hash(sym) % 50,
                    volume=1000000,
                )
                for sym in symbols
            ]

        # Safely import and patch YahooFinanceClient
        try:
            from shared.clients.market_data import YahooFinanceClient
        except ImportError:
            import sys

            sys.path.insert(0, "src/lambda/layers/common/python")
            from shared.clients.market_data import YahooFinanceClient

        monkeypatch.setattr(YahooFinanceClient, "fetch_prices", mock_fetch_prices)

    return _stub_yahoo_finance
