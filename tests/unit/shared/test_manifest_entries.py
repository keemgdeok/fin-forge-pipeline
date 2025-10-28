from __future__ import annotations

import json

import boto3
from moto import mock_aws

from shared.ingestion.manifests import (
    ManifestEntry,
    collect_manifest_entries,
    persist_chunk_summary,
)


@mock_aws
def test_collect_manifest_entries_prefers_tracker_manifest_keys() -> None:
    """
    Given: 배치 트래커에 manifest_keys가 저장된 상태
    When: collect_manifest_entries 호출
    Then: 트래커 manifest가 우선 반환
    """
    raw_bucket = "test-manifest-bucket"
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=raw_bucket)

    manifest_key = "market/prices/interval=1d/data_source=yahoo_finance/year=2025/month=09/day=10/_batch.manifest.json"
    s3.put_object(
        Bucket=raw_bucket,
        Key=manifest_key,
        Body=json.dumps({"ds": "2025-09-10"}).encode("utf-8"),
    )

    table_name = "test-batch-tracker"
    dynamodb_client = boto3.client("dynamodb", region_name="us-east-1")
    dynamodb_client.create_table(
        TableName=table_name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )

    table = boto3.resource("dynamodb", region_name="us-east-1").Table(table_name)
    table.put_item(
        Item={
            "pk": "batch-1",
            "manifest_keys": [manifest_key],
        }
    )

    entries = collect_manifest_entries(
        batch_id="batch-1",
        raw_bucket=raw_bucket,
        domain="market",
        table_name="prices",
        interval="1d",
        data_source="yahoo_finance",
        tracker_table=table,
    )

    assert entries == [ManifestEntry(ds="2025-09-10", manifest_key=manifest_key, source="dynamodb")]


@mock_aws
def test_collect_manifest_entries_falls_back_to_chunk_summaries() -> None:
    """
    Given: 트래커에 키가 없고 청크 요약만 존재
    When: collect_manifest_entries 호출
    Then: 청크 요약 기반 manifest가 반환
    """
    raw_bucket = "test-manifest-bucket"
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=raw_bucket)

    raw_prefix = "market/prices/interval=1d/data_source=yahoo_finance/year=2025/month=09/day=11/"
    s3.put_object(Bucket=raw_bucket, Key=f"{raw_prefix}AAPL.json", Body=b"{}")

    persist_chunk_summary(
        raw_bucket=raw_bucket,
        batch_id="batch-2",
        partition_summaries=[
            {
                "ds": "2025-09-11",
                "raw_prefix": raw_prefix,
                "objects": [{"key": f"{raw_prefix}AAPL.json"}],
            }
        ],
    )

    expected_manifest = (
        "market/prices/interval=1d/data_source=yahoo_finance/year=2025/month=09/day=11/_batch.manifest.json"
    )

    entries = collect_manifest_entries(
        batch_id="batch-2",
        raw_bucket=raw_bucket,
        domain="market",
        table_name="prices",
        interval="1d",
        data_source="yahoo_finance",
        tracker_table=None,
    )

    assert entries == [ManifestEntry(ds="2025-09-11", manifest_key=expected_manifest, source="chunk_summary")]
