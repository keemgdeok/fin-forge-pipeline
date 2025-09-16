from __future__ import annotations

import json
from typing import Dict, cast

import pytest
from moto import mock_aws

from tests.fixtures.load_builders import build_queue_names


pytestmark = [pytest.mark.unit, pytest.mark.infrastructure, pytest.mark.load]


@mock_aws
def test_queue_naming_and_redrive_policy(make_load_queues) -> None:
    env = "dev"
    domain = "market"
    main_url, dlq_url = make_load_queues(env, domain)

    # Given: Load용 메인 큐와 DLQ가 생성되고
    # When: 큐 이름 및 속성을 조회하면
    # Then: 스펙의 네이밍 규칙과 redrive 설정을 만족해야 한다
    # Assert queue names follow the spec
    expected = build_queue_names(env=env, domain=domain)

    import boto3

    sqs = boto3.client("sqs", region_name="us-east-1")

    def _name(url: str) -> str:
        response: Dict[str, Dict[str, str]] = sqs.get_queue_attributes(  # type: ignore[arg-type]
            QueueUrl=url,
            AttributeNames=["QueueArn"],
        )
        attributes = cast(Dict[str, str], response.get("Attributes", {}))
        arn = attributes["QueueArn"]
        return arn.split(":")[-1]

    assert _name(main_url) == expected.main
    assert _name(dlq_url) == expected.dlq

    # Check redrive policy on main queue
    attrs = sqs.get_queue_attributes(
        QueueUrl=main_url,
        AttributeNames=["RedrivePolicy", "VisibilityTimeout", "MessageRetentionPeriod"],
    )
    attributes = cast(Dict[str, str], attrs.get("Attributes", {}))
    redrive = json.loads(attributes["RedrivePolicy"])
    assert redrive["maxReceiveCount"] == 3
    assert int(attributes["VisibilityTimeout"]) == 1800
    assert int(attributes["MessageRetentionPeriod"]) == 14 * 24 * 60 * 60
