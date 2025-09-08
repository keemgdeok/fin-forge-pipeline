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
        raw_bucket: str = None,
        curated_bucket: str = None,
        artifacts_bucket: str = None,
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


@pytest.fixture
def preflight_module() -> Callable[[], dict[str, Any]]:
    """Load preflight Lambda module for testing."""
    import runpy

    def _load() -> dict[str, Any]:
        return runpy.run_path("src/lambda/functions/preflight/handler.py")

    return _load


@pytest.fixture
def schema_check_module() -> Callable[[], dict[str, Any]]:
    """Load schema check Lambda module for testing."""
    import runpy

    def _load() -> dict[str, Any]:
        return runpy.run_path("src/lambda/functions/schema_check/handler.py")

    return _load


@pytest.fixture
def build_dates_module() -> Callable[[], dict[str, Any]]:
    """Load build dates Lambda module for testing."""
    import runpy

    def _load() -> dict[str, Any]:
        return runpy.run_path("src/lambda/functions/build_dates/handler.py")

    return _load


def pytest_configure(config):
    """Configure pytest with custom markers for transform testing."""
    # Add transform-specific markers
    config.addinivalue_line("markers", "unit: unit test")
    config.addinivalue_line("markers", "integration: integration test")
    config.addinivalue_line("markers", "slow: slow running test")
    config.addinivalue_line("markers", "transform: transform pipeline test")
    config.addinivalue_line("markers", "lambda_test: Lambda function test")
    config.addinivalue_line("markers", "glue: Glue job test")
    config.addinivalue_line("markers", "infrastructure: Infrastructure/CDK test")
    config.addinivalue_line("markers", "preflight: Preflight Lambda tests")
    config.addinivalue_line("markers", "schema_check: Schema check Lambda tests")
    config.addinivalue_line("markers", "build_dates: Build dates Lambda tests")
    config.addinivalue_line("markers", "data_quality: Data quality validation tests")
    config.addinivalue_line("markers", "error_contract: Error contract validation tests")


def pytest_addoption(parser):
    """Add custom command line options for transform testing."""
    parser.addoption("--runslow", action="store_true", default=False, help="run slow tests")
    parser.addoption(
        "--component",
        action="store",
        default=None,
        help="run tests for specific component (lambda, glue, infrastructure)",
    )
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

    # Filter by component if specified
    component = item.config.getoption("--component")
    if component:
        if component not in [mark.name for mark in item.iter_markers()]:
            pytest.skip(f"test not in component {component}")

    # Run only transform tests if requested
    if item.config.getoption("--transform-only"):
        if not any(
            mark.name
            in [
                "transform",
                "preflight",
                "schema_check",
                "build_dates",
                "glue",
            ]
            for mark in item.iter_markers()
        ):
            pytest.skip("not a transform pipeline test")
