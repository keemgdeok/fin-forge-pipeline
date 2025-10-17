from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, List

from tests.e2e.utils.env import LocalStackConfig


def create_bucket(config: LocalStackConfig, name: str) -> str:
    config.client("s3").create_bucket(Bucket=name)
    return name


def create_queue(config: LocalStackConfig, name: str) -> tuple[str, str]:
    client = config.client("sqs")
    queue_url = client.create_queue(QueueName=name)["QueueUrl"]
    attrs = client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
    return queue_url, attrs["Attributes"]["QueueArn"]


def create_batch_tracker_table(config: LocalStackConfig, name: str) -> None:
    table = config.resource("dynamodb").create_table(
        TableName=name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )
    table.wait_until_exists()


def store_symbol_universe(config: LocalStackConfig, param_name: str, symbols: Iterable[str]) -> None:
    config.client("ssm").put_parameter(Name=param_name, Value=json.dumps(list(symbols)), Type="String", Overwrite=True)


def receive_all_messages(config: LocalStackConfig, queue_url: str) -> List[Dict[str, Any]]:
    client = config.client("sqs")
    messages: List[Dict[str, Any]] = []
    while True:
        resp = client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
            MessageAttributeNames=["All"],
            AttributeNames=["All"],
        )
        batch = resp.get("Messages", [])
        if not batch:
            break
        messages.extend(batch)
    return messages


def delete_messages(config: LocalStackConfig, queue_url: str, messages: Iterable[Dict[str, Any]]) -> None:
    client = config.client("sqs")
    for message in messages:
        client.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])


def build_sqs_event(messages: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    records: List[Dict[str, Any]] = []
    for message in messages:
        records.append(
            {
                "messageId": message["MessageId"],
                "body": message["Body"],
                "attributes": message.get("Attributes", {}),
                "messageAttributes": message.get("MessageAttributes", {}),
            }
        )
    return {"Records": records}


def list_objects(config: LocalStackConfig, bucket: str, prefix: str) -> List[str]:
    client = config.client("s3")
    paginator = client.get_paginator("list_objects_v2")
    keys: List[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def parse_manifest_ds(manifest_key: str) -> str:
    match = re.search(r"year=(\d{4})/month=(\d{2})/day=(\d{2})", manifest_key)
    if not match:
        raise AssertionError(f"Unexpected manifest key format: {manifest_key}")
    year, month, day = match.groups()
    return f"{year}-{month}-{day}"
