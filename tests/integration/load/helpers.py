from __future__ import annotations

import json
from typing import Any, Callable, Dict, List, Tuple

import boto3

from tests.fixtures.load_agent import FakeLoaderAgent, LoaderConfig

CURATED_BUCKET = "data-pipeline-curated-dev"
CURATED_DOMAIN = "market"
CURATED_TABLE = "prices"
CURATED_INTERVAL = "1d"
CURATED_DATA_SOURCE = "yahoo"
CURATED_YEAR = "2025"
CURATED_MONTH = "09"
CURATED_DAY = "10"
CURATED_LAYER = "adjusted"
CURATED_DS = "2025-09-10"


def build_key(
    *,
    domain: str = CURATED_DOMAIN,
    table: str = CURATED_TABLE,
    interval: str = CURATED_INTERVAL,
    data_source: str | None = CURATED_DATA_SOURCE,
    year: str = CURATED_YEAR,
    month: str = CURATED_MONTH,
    day: str = CURATED_DAY,
    layer: str = CURATED_LAYER,
    part: str,
) -> str:
    segments = [domain, table, f"interval={interval}"]
    if data_source:
        segments.append(f"data_source={data_source}")
    segments.extend([f"year={year}", f"month={month}", f"day={day}", f"layer={layer}", part])
    return "/".join(segments)


def build_message(
    *,
    part: str,
    correlation_id: str,
    domain: str = CURATED_DOMAIN,
    data_source: str | None = CURATED_DATA_SOURCE,
    file_size: int | None = None,
) -> Dict[str, Any]:
    message: Dict[str, Any] = {
        "bucket": CURATED_BUCKET,
        "key": build_key(domain=domain, data_source=data_source, part=part),
        "domain": domain,
        "table_name": CURATED_TABLE,
        "interval": CURATED_INTERVAL,
        "layer": CURATED_LAYER,
        "year": CURATED_YEAR,
        "month": CURATED_MONTH,
        "day": CURATED_DAY,
        "ds": CURATED_DS,
        "correlation_id": correlation_id,
    }
    if data_source is not None:
        message["data_source"] = data_source
    if file_size is not None:
        message["file_size"] = file_size
    return message


def prepare_queue(make_load_queues, *, visibility_timeout: int | None = None) -> Tuple[Any, str, str]:
    main_url, dlq_url = make_load_queues("dev", "market")
    sqs = boto3.client("sqs", region_name="us-east-1")
    if visibility_timeout is not None:
        sqs.set_queue_attributes(QueueUrl=main_url, Attributes={"VisibilityTimeout": str(visibility_timeout)})
    return sqs, main_url, dlq_url


def drain_main_queue(
    *,
    sqs_client: Any,
    main_url: str,
    dlq_url: str,
    max_attempts: int = 4,
    visibility_timeout: int = 0,
) -> List[Dict[str, Any]]:
    for _ in range(max_attempts):
        response = sqs_client.receive_message(
            QueueUrl=main_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=1,
            AttributeNames=["ApproximateReceiveCount"],
        )
        messages = response.get("Messages", [])
        if not messages:
            break
        sqs_client.change_message_visibility(
            QueueUrl=main_url,
            ReceiptHandle=messages[0]["ReceiptHandle"],
            VisibilityTimeout=visibility_timeout,
        )

    dlq_response = sqs_client.receive_message(QueueUrl=dlq_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    return dlq_response.get("Messages", [])


def run_dlq_scenario(
    make_load_queues,
    *,
    part: str,
    process: Callable[[Dict[str, Any]], str],
    expected_code: str,
    correlation_id: str = "dead-letter",
    attempts: int = 3,
) -> List[Dict[str, Any]]:
    sqs, main_url, dlq_url = prepare_queue(make_load_queues, visibility_timeout=0)
    sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps(build_message(part=part, correlation_id=correlation_id)))

    agent = FakeLoaderAgent(LoaderConfig(queue_url=main_url, retry_visibility_timeout=0))

    for _ in range(attempts):
        result = agent.run_once(process)
        codes = [
            entry.get("error_code") for entry in result["results"] if entry["action"] in {"EXCEPTION", "PARSE_ERROR"}
        ]
        assert codes and codes[0] == expected_code

    return drain_main_queue(sqs_client=sqs, main_url=main_url, dlq_url=dlq_url)
