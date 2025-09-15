from __future__ import annotations

import json
from typing import Any, Dict

import pytest
from moto import mock_aws

from tests.fixtures.load_agent import FakeLoaderAgent, LoaderConfig


pytestmark = [pytest.mark.integration, pytest.mark.load]


@mock_aws
def test_happy_path_ack_deletes_message(make_load_queues) -> None:
    env = "dev"
    domain = "market"
    main_url, _ = make_load_queues(env, domain)

    import boto3

    sqs = boto3.client("sqs", region_name="us-east-1")
    body = {
        "bucket": "data-pipeline-curated-dev",
        "key": "market/prices/ds=2025-09-10/part-001.parquet",
        "domain": "market",
        "table_name": "prices",
        "partition": "ds=2025-09-10",
        "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
    }
    sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps(body))

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url))

    def process(msg: Dict[str, Any]) -> str:
        assert msg["domain"] == "market"
        return "SUCCESS"

    result = agent.run_once(process)
    assert result["count"] == 1

    # Queue should be empty after ACK
    resp = sqs.receive_message(QueueUrl=main_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
    assert resp.get("Messages", []) == []


@mock_aws
def test_partial_batch_retry_then_success(make_load_queues) -> None:
    env = "dev"
    domain = "market"
    main_url, _ = make_load_queues(env, domain)

    import boto3

    sqs = boto3.client("sqs", region_name="us-east-1")
    good = {
        "bucket": "data-pipeline-curated-dev",
        "key": "market/prices/ds=2025-09-10/part-001.parquet",
        "domain": "market",
        "table_name": "prices",
        "partition": "ds=2025-09-10",
        "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
    }
    retry = {
        "bucket": "data-pipeline-curated-dev",
        "key": "market/prices/ds=2025-09-10/part-002.parquet",
        "domain": "market",
        "table_name": "prices",
        "partition": "ds=2025-09-10",
        "correlation_id": "660e8400-e29b-41d4-a716-446655440001",
    }
    for msg in [good, retry]:
        sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps(msg))

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url))

    def process_first(msg: Dict[str, Any]) -> str:
        return "RETRY" if msg["key"].endswith("part-002.parquet") else "SUCCESS"

    # First pass: 1 success, 1 retry (visibility set to 0 to requeue)
    r1 = agent.run_once(process_first)
    assert r1["count"] == 2

    # Second pass: process the retried message successfully
    def process_second(msg: Dict[str, Any]) -> str:
        return "SUCCESS"

    r2 = agent.run_once(process_second)
    assert r2["count"] >= 1  # at least the retried message

    # Queue should be empty after second pass
    resp = sqs.receive_message(QueueUrl=main_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
    assert resp.get("Messages", []) == []
