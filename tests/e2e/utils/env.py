from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import boto3
import pytest
from botocore.exceptions import EndpointConnectionError


DEFAULT_ENDPOINT = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566")
DEFAULT_INTERNAL_ENDPOINT = os.environ.get("LOCALSTACK_INTERNAL_ENDPOINT", "http://localhost.localstack.cloud:4566")
DEFAULT_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
DEFAULT_ACCOUNT_ID = os.environ.get("LOCALSTACK_ACCOUNT_ID", "000000000000")


@dataclass(frozen=True)
class LocalStackConfig:
    endpoint: str = DEFAULT_ENDPOINT
    internal_endpoint: str = DEFAULT_INTERNAL_ENDPOINT
    region: str = DEFAULT_REGION
    account_id: str = DEFAULT_ACCOUNT_ID
    access_key: str = "test"
    secret_key: str = "test"

    def client(self, service_name: str, *, internal: bool = False, **overrides: str):
        endpoint = self.internal_endpoint if internal else self.endpoint
        return boto3.client(
            service_name,
            endpoint_url=overrides.get("endpoint_url", endpoint),
            region_name=overrides.get("region_name", self.region),
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

    def resource(self, service_name: str, *, internal: bool = False, **overrides: str):
        endpoint = self.internal_endpoint if internal else self.endpoint
        return boto3.resource(
            service_name,
            endpoint_url=overrides.get("endpoint_url", endpoint),
            region_name=overrides.get("region_name", self.region),
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )


def ensure_localstack_available(config: LocalStackConfig | None = None) -> None:
    cfg = config or LocalStackConfig()
    try:
        cfg.client("s3").list_buckets()
    except EndpointConnectionError:
        pytest.skip("LocalStack endpoint not reachable; skipping LocalStack-based tests")


def ensure_load_contracts_path() -> None:
    layer_root = Path(__file__).resolve().parents[3] / "src" / "lambda" / "layers" / "load" / "contracts" / "python"
    layer_str = str(layer_root)
    if layer_str not in sys.path:
        sys.path.insert(0, layer_str)


def patch_boto3(monkeypatch: pytest.MonkeyPatch, config: LocalStackConfig) -> None:
    original_client = boto3.client
    original_resource = boto3.resource

    def _client(service_name: str, *args, **kwargs):
        kwargs.setdefault("endpoint_url", config.endpoint)
        kwargs.setdefault("region_name", config.region)
        kwargs.setdefault("aws_access_key_id", config.access_key)
        kwargs.setdefault("aws_secret_access_key", config.secret_key)
        return original_client(service_name, *args, **kwargs)

    def _resource(service_name: str, *args, **kwargs):
        kwargs.setdefault("endpoint_url", config.endpoint)
        kwargs.setdefault("region_name", config.region)
        kwargs.setdefault("aws_access_key_id", config.access_key)
        kwargs.setdefault("aws_secret_access_key", config.secret_key)
        return original_resource(service_name, *args, **kwargs)

    monkeypatch.setattr(boto3, "client", _client)
    monkeypatch.setattr(boto3, "resource", _resource)


@contextmanager
def localstack_env(monkeypatch: pytest.MonkeyPatch, config: LocalStackConfig) -> Iterator[None]:
    monkeypatch.setenv("LOCALSTACK_ENDPOINT", config.endpoint)
    monkeypatch.setenv("LOCALSTACK_INTERNAL_ENDPOINT", config.internal_endpoint)
    monkeypatch.setenv("AWS_REGION", config.region)
    monkeypatch.setenv("AWS_DEFAULT_REGION", config.region)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", config.access_key)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", config.secret_key)
    yield


def apply_localstack_env(monkeypatch: pytest.MonkeyPatch, config: LocalStackConfig) -> None:
    monkeypatch.setenv("LOCALSTACK_ENDPOINT", config.endpoint)
    monkeypatch.setenv("LOCALSTACK_INTERNAL_ENDPOINT", config.internal_endpoint)
    monkeypatch.setenv("AWS_REGION", config.region)
    monkeypatch.setenv("AWS_DEFAULT_REGION", config.region)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", config.access_key)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", config.secret_key)
