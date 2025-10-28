from __future__ import annotations

import json
import runpy
from decimal import Decimal

import boto3
from moto import mock_aws

handler_module = runpy.run_path("src/lambda/functions/ingestion_worker/handler.py")

_update_batch_tracker = handler_module["_update_batch_tracker"]
_persist_chunk_summary = handler_module["_persist_chunk_summary"]
_load_chunk_summaries = handler_module["_load_chunk_summaries"]
_cleanup_chunk_summaries = handler_module["_cleanup_chunk_summaries"]
_emit_manifests = handler_module["_emit_manifests"]
_logger = handler_module["logger"]


def _create_batch_table(table_name: str) -> None:
    dynamodb = boto3.client("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        TableName=table_name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )


@mock_aws
def test_update_batch_tracker_summarizes_objects() -> None:
    """
    Given: 처리된 청크가 없는 기존 배치 트래커 아이템이 존재
    When: _update_batch_tracker 호출로 파티션 요약을 축적하면
    Then: 배치가 완료되지 않은 상태로 객체 정보가 요약에 합쳐져야 함
    """
    table_name = "test-batch-tracker"
    _create_batch_table(table_name)

    table = boto3.resource("dynamodb", region_name="us-east-1").Table(table_name)
    table.put_item(
        Item={
            "pk": "batch-1",
            "expected_chunks": Decimal(2),
            "processed_chunks": Decimal(0),
            "status": "processing",
        }
    )

    partition_summaries = [
        {
            "ds": "2025-09-10",
            "raw_prefix": "market/prices/interval=1d/data_source=yahoo_finance/year=2025/month=09/day=10/",
            "objects": [
                {"symbol": "AAPL", "key": "key-1", "records": 5},
                {"symbol": "MSFT", "key": "key-2", "records": 5},
            ],
        }
    ]

    should_finalize, attrs = _update_batch_tracker(
        table_name=table_name,
        batch_id="batch-1",
        batch_ds="2025-09-10",
        partition_summaries=partition_summaries,
        payload={"symbols": ["AAPL", "MSFT"]},
        log=_logger,
    )

    assert not should_finalize
    stored = table.get_item(Key={"pk": "batch-1"})["Item"]
    payload_entry = stored["partition_payload"][0]
    assert payload_entry["object_count"] == 2
    assert "objects" not in payload_entry
    assert "combined_partition_summaries" not in attrs


@mock_aws
def test_chunk_summary_roundtrip_and_manifest_generation() -> None:
    """
    Given: S3에 저장된 청크 요약과 원본 객체가 존재
    When: 요약을 읽어 와서 매니페스트를 생성하면
    Then: 매니페스트가 원본 객체 키를 포함하고 임시 프리픽스에는 객체가 없어야 함
    """
    raw_bucket = "test-raw-bucket"
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=raw_bucket)

    raw_prefix = "market/prices/interval=1d/data_source=yahoo_finance/year=2025/month=09/day=10/"
    s3_key = f"{raw_prefix}AAPL.json"
    s3.put_object(Bucket=raw_bucket, Key=s3_key, Body=b"{}")

    _persist_chunk_summary(
        raw_bucket=raw_bucket,
        batch_id="batch-emit",
        partition_summaries=[{"ds": "2025-09-10", "raw_prefix": raw_prefix, "objects": []}],
        log=_logger,
    )

    manifest_keys = _emit_manifests(
        raw_bucket=raw_bucket,
        manifest_basename="_batch",
        manifest_suffix=".manifest.json",
        environment="dev",
        batch_id="batch-emit",
        payload={
            "domain": "market",
            "table_name": "prices",
            "data_source": "yahoo_finance",
            "interval": "1d",
        },
        tracker_attrs={},
        partition_entries=[],
        log=_logger,
    )

    assert len(manifest_keys) == 1
    manifest_key = manifest_keys[0]
    manifest = json.loads(s3.get_object(Bucket=raw_bucket, Key=manifest_key)["Body"].read().decode("utf-8"))
    assert manifest["objects"][0]["key"] == s3_key

    resp = s3.list_objects_v2(Bucket=raw_bucket, Prefix="manifests/tmp/batch-emit/")
    assert resp.get("KeyCount", 0) == 0
