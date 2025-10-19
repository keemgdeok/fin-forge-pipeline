from __future__ import annotations

import pytest

from tests.e2e.utils.env import (
    LocalStackConfig,
    apply_localstack_env,
    ensure_load_contracts_path,
    ensure_localstack_available,
    patch_boto3,
)
from tests.e2e.utils.workflow import WorkflowDeployment, deploy_transform_workflow, destroy_transform_workflow

# Mark all tests in this package as end-to-end tests
pytestmark = pytest.mark.e2e


@pytest.fixture(scope="session")
def localstack_config() -> LocalStackConfig:
    """Session-scoped LocalStack 설정."""
    config = LocalStackConfig()
    ensure_localstack_available(config)
    return config


@pytest.fixture(autouse=True)
def _localstack_bootstrap(monkeypatch: pytest.MonkeyPatch, localstack_config: LocalStackConfig) -> None:
    """각 테스트 전에 boto3 패치 및 환경 변수를 주입한다."""
    patch_boto3(monkeypatch, localstack_config)
    apply_localstack_env(monkeypatch, localstack_config)
    ensure_load_contracts_path()
    yield


@pytest.fixture(scope="session")
def transform_workflow(localstack_config: LocalStackConfig) -> WorkflowDeployment:
    """변환 워크플로를 세션 스코프로 배포하고 테스트 종료 시 정리한다."""
    deployment = deploy_transform_workflow(localstack_config)
    try:
        yield deployment
    finally:
        destroy_transform_workflow(localstack_config, deployment)
