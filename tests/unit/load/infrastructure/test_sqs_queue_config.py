from __future__ import annotations

import json
from typing import Dict

import pytest
from moto import mock_aws

from tests.fixtures.load_builders import build_queue_names


pytestmark = [pytest.mark.unit, pytest.mark.infrastructure, pytest.mark.load]


@mock_aws
def test_queue_naming_and_redrive_policy(make_load_queues) -> None:
    env = "dev"
    domain = "market"
    main_url, dlq_url = make_load_queues(env, domain)

    # Assert queue names follow the spec
    expected = build_queue_names(env=env, domain=domain)

    import boto3

    sqs = boto3.client("sqs", region_name="us-east-1")

    def _name(url: str) -> str:
        attrs: Dict[str, str] = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])  # type: ignore[arg-type]
        arn = attrs["Attributes"]["QueueArn"]
        return arn.split(":")[-1]

    assert _name(main_url) == expected.main
    assert _name(dlq_url) == expected.dlq

    # Check redrive policy on main queue
    attrs = sqs.get_queue_attributes(
        QueueUrl=main_url,
        AttributeNames=["RedrivePolicy", "VisibilityTimeout", "MessageRetentionPeriod"],
    )
    redrive = json.loads(attrs["Attributes"]["RedrivePolicy"])  # type: ignore[index]
    assert redrive["maxReceiveCount"] == 3
    assert int(attrs["Attributes"]["VisibilityTimeout"]) == 1800  # type: ignore[index]
    assert int(attrs["Attributes"]["MessageRetentionPeriod"]) == 14 * 24 * 60 * 60  # type: ignore[index]
