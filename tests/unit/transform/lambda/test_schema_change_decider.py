"""Unit tests for SchemaChangeDecider Lambda.

Covers policy handling (never/force/on_schema_change) and hash comparisons
between latest.json and previous.json in the schema fingerprint path.
"""

import json
from typing import Dict
from importlib import import_module

import boto3
import pytest
from moto import mock_s3


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


def _event(bucket: str, key: str, policy: str | None = None) -> Dict:
    e: Dict = {"glue_args": {"--schema_fingerprint_s3_uri": f"s3://{bucket}/{key}"}}
    if policy:
        e["catalog_update"] = policy
    return e


@mock_s3
def test_policy_never_returns_false(monkeypatch: pytest.MonkeyPatch) -> None:
    lambda_handler = import_module("src.lambda.functions.schema_change_decider.handler").lambda_handler

    s3 = boto3.client("s3", region_name="us-east-1")
    bucket = "test-artifacts"
    key = "market/prices/_schema/latest.json"
    s3.create_bucket(Bucket=bucket)

    event = _event(bucket, key, policy="never")
    out = lambda_handler(event, {})
    assert out == {"shouldRunCrawler": False}


@mock_s3
def test_policy_force_returns_true(monkeypatch: pytest.MonkeyPatch) -> None:
    lambda_handler = import_module("src.lambda.functions.schema_change_decider.handler").lambda_handler

    s3 = boto3.client("s3", region_name="us-east-1")
    bucket = "test-artifacts"
    key = "market/prices/_schema/latest.json"
    s3.create_bucket(Bucket=bucket)

    event = _event(bucket, key, policy="force")
    out = lambda_handler(event, {})
    assert out == {"shouldRunCrawler": True}


@mock_s3
def test_on_schema_change_no_latest_returns_true(monkeypatch: pytest.MonkeyPatch) -> None:
    lambda_handler = import_module("src.lambda.functions.schema_change_decider.handler").lambda_handler

    s3 = boto3.client("s3", region_name="us-east-1")
    bucket = "test-artifacts"
    key = "market/prices/_schema/latest.json"
    s3.create_bucket(Bucket=bucket)

    event = _event(bucket, key, policy="on_schema_change")
    out = lambda_handler(event, {})
    assert out == {"shouldRunCrawler": True}


@mock_s3
def test_on_schema_change_equal_hash_returns_false(monkeypatch: pytest.MonkeyPatch) -> None:
    lambda_handler = import_module("src.lambda.functions.schema_change_decider.handler").lambda_handler

    s3 = boto3.client("s3", region_name="us-east-1")
    bucket = "test-artifacts"
    base = "market/prices/_schema"
    latest_key = f"{base}/latest.json"
    previous_key = f"{base}/previous.json"
    s3.create_bucket(Bucket=bucket)

    payload = {"columns": [{"name": "symbol", "type": "string"}], "codec": "zstd", "hash": "abc"}
    body = json.dumps(payload).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=latest_key, Body=body, ContentType="application/json")
    s3.put_object(Bucket=bucket, Key=previous_key, Body=body, ContentType="application/json")

    event = _event(bucket, latest_key, policy="on_schema_change")
    out = lambda_handler(event, {})
    assert out == {"shouldRunCrawler": False}


@mock_s3
def test_on_schema_change_diff_hash_returns_true(monkeypatch: pytest.MonkeyPatch) -> None:
    lambda_handler = import_module("src.lambda.functions.schema_change_decider.handler").lambda_handler

    s3 = boto3.client("s3", region_name="us-east-1")
    bucket = "test-artifacts"
    base = "market/prices/_schema"
    latest_key = f"{base}/latest.json"
    previous_key = f"{base}/previous.json"
    s3.create_bucket(Bucket=bucket)

    latest = {"columns": [{"name": "symbol", "type": "string"}], "codec": "zstd", "hash": "new"}
    previous = {"columns": [{"name": "symbol", "type": "string"}], "codec": "zstd", "hash": "old"}
    s3.put_object(
        Bucket=bucket, Key=latest_key, Body=json.dumps(latest).encode("utf-8"), ContentType="application/json"
    )
    s3.put_object(
        Bucket=bucket, Key=previous_key, Body=json.dumps(previous).encode("utf-8"), ContentType="application/json"
    )

    event = _event(bucket, latest_key, policy="on_schema_change")
    out = lambda_handler(event, {})
    assert out == {"shouldRunCrawler": True}
