"""Environment and AWS resource helpers for Load pipeline tests.

Uses moto/boto3 to provision SQS queues (main + DLQ) per spec.
"""

from __future__ import annotations

import json
import os
from typing import Callable, Tuple

import boto3
import pytest


@pytest.fixture
def sqs_client() -> boto3.client:
    """Provide an SQS client bound to the default test region."""
    region = os.environ.get("AWS_REGION", "us-east-1")
    return boto3.client("sqs", region_name=region)


@pytest.fixture
def load_env(monkeypatch: pytest.MonkeyPatch) -> Callable[[str, str], None]:
    """Set basic environment variables for load tests."""

    def _apply(env: str, domain: str) -> None:
        monkeypatch.setenv("ENVIRONMENT", env)
        monkeypatch.setenv("LOAD_DOMAIN", domain)
        monkeypatch.setenv("AWS_REGION", os.environ.get("AWS_REGION", "us-east-1"))

    return _apply


@pytest.fixture
def make_load_queues(sqs_client) -> Callable[[str, str], Tuple[str, str]]:
    """Create main and DLQ queues with redrive policy per spec.

    Returns a tuple of (main_queue_url, dlq_url).
    """

    def _create(env: str, domain: str) -> Tuple[str, str]:
        main_name = f"{env}-{domain}-load-queue"
        dlq_name = f"{env}-{domain}-load-dlq"

        # Create DLQ first
        dlq_url = sqs_client.create_queue(QueueName=dlq_name)["QueueUrl"]
        dlq_attrs = sqs_client.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])  # type: ignore[arg-type]
        dlq_arn = dlq_attrs["Attributes"]["QueueArn"]

        # Create main queue with redrive policy and required attributes
        redrive = json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": 3})
        main_url = sqs_client.create_queue(
            QueueName=main_name,
            Attributes={
                "RedrivePolicy": redrive,
                # Spec: VisibilityTimeout 1800s (≥ 6× per-message timeout 300s)
                "VisibilityTimeout": "1800",
                # Spec: Message Retention 14 days
                "MessageRetentionPeriod": str(14 * 24 * 60 * 60),
            },
        )["QueueUrl"]
        return main_url, dlq_url

    return _create
