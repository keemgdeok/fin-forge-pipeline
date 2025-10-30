"""
Manifest-driven Transform Pipeline Integration Test

이 테스트는 인제스트 결과로 생성된 RAW 객체와 청크 요약을 활용해
Step Functions 실행 입력까지 이어지는 실제 코드 경로를 검증합니다.

검증 대상:
1. `_persist_records`가 RAW 버킷에 심볼별 객체를 작성하고 청크 요약 메타데이터를 반환
2. `persist_chunk_summary`와 `collect_manifest_entries`가 manifest 키를 도출
3. `start_transform_execution`이 Step Functions에 전달하는 payload에 올바른 manifest 정보가 포함
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Dict, List

import boto3
import pytest
from moto import mock_aws

from market_shared.clients import PriceRecord
from market_shared.ingestion.service import MarketDataIngestionService
from shared.ingestion.manifests import collect_manifest_entries, persist_chunk_summary
from src.step_functions.workflows.runner import (
    ManifestItem,
    TransformExecutionInput,
    start_transform_execution,
)

pytestmark = pytest.mark.integration


def _build_price_record(symbol: str, close: float) -> PriceRecord:
    """편의를 위한 PriceRecord 생성기."""
    return PriceRecord(
        symbol=symbol,
        timestamp=datetime(2025, 9, 7, 12, 0, tzinfo=timezone.utc),
        close=close,
        adjusted_close=close + 1.0,
        open=close - 1.0,
        high=close + 2.0,
        low=close - 2.0,
        volume=1_000_000,
    )


@mock_aws
def test_ingestion_to_stepfunctions_manifest_flow(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Given: 세 개 심볼에 대한 PriceRecord 목록과 RAW 버킷이 준비되어 있으면
    When: 인제스트 서비스가 데이터를 저장하고 manifest를 수집한 뒤 실행 입력을 생성하면
    Then: Step Functions 실행 요청에 올바른 manifest 목록과 메타데이터가 포함되어야 함
    """

    raw_bucket = "test-raw-bucket"
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket=raw_bucket)

    # 인제스트 서비스가 사용하는 환경 변수 설정
    monkeypatch.setenv("RAW_BUCKET", raw_bucket)
    monkeypatch.setenv("ENVIRONMENT", "dev")

    service = MarketDataIngestionService(data_client=None)  # data_client는 _persist_records에서 사용하지 않음

    records: List[PriceRecord] = [
        _build_price_record("AAPL", 150.0),
        _build_price_record("MSFT", 310.0),
        _build_price_record("GOOG", 2800.0),
    ]

    # RAW S3에 객체 작성 및 manifest 메타데이터 획득
    ingestion_result = service._persist_records(
        fetched=records,
        raw_bucket=raw_bucket,
        domain="market",
        table_name="prices",
        data_source="yahoo_finance",
        interval="1d",
        file_format="json",
    )

    batch_id = "batch-1"

    # 청크 요약을 S3에 저장 (일반적으로 워커가 수행)
    for partition_day, summary in ingestion_result.manifest_objects.items():
        persist_chunk_summary(
            raw_bucket=raw_bucket,
            batch_id=batch_id,
            partition_summaries=[
                {
                    "ds": partition_day.isoformat(),
                    "raw_prefix": summary.get("raw_prefix", ""),
                    "objects": summary.get("objects", []),
                }
            ],
        )

    # 저장된 RAW 객체 수 확인 (심볼당 1개 파일)
    listed_raw = s3_client.list_objects_v2(Bucket=raw_bucket, Prefix="market/prices/")
    assert listed_raw.get("KeyCount", 0) == len(records), "인제스트 결과 RAW 파일 수가 기대와 다릅니다"

    # 청크 요약 기반 manifest entry 추출
    manifest_entries = collect_manifest_entries(
        batch_id=batch_id,
        raw_bucket=raw_bucket,
        domain="market",
        table_name="prices",
        interval="1d",
        data_source="yahoo_finance",
    )
    assert manifest_entries, "Manifest 엔트리가 생성되지 않았습니다"

    manifest_items = [
        ManifestItem(ds=entry.ds, manifest_key=entry.manifest_key, source=entry.source) for entry in manifest_entries
    ]

    execution_input = TransformExecutionInput(
        manifest_items=manifest_items,
        domain="market",
        table_name="prices",
        raw_bucket=raw_bucket,
        interval="1d",
        data_source="yahoo_finance",
        environment="dev",
        batch_id=batch_id,
    )

    captured: Dict[str, Dict[str, str]] = {}

    class _StubStepFunctionsClient:
        def start_execution(self, **kwargs):
            captured["kwargs"] = kwargs
            return {"executionArn": "arn:aws:states:us-east-1:123456789012:execution:test:demo"}

    monkeypatch.setattr(
        "src.step_functions.workflows.runner.boto3.client",
        lambda service, region_name=None: _StubStepFunctionsClient(),
    )

    sm_arn = "arn:aws:states:us-east-1:123456789012:stateMachine:test-transform"
    start_transform_execution(sm_arn=sm_arn, payload=execution_input)

    assert "kwargs" in captured, "Step Functions 실행이 호출되지 않았습니다"
    start_args = captured["kwargs"]

    assert start_args["stateMachineArn"] == sm_arn
    payload = json.loads(start_args["input"])

    assert payload["domain"] == "market"
    assert payload["table_name"] == "prices"
    assert payload["raw_bucket"] == raw_bucket
    assert payload["batch_id"] == batch_id
    assert len(payload["manifest_keys"]) == len(manifest_items)

    manifest_keys = [item["manifest_key"] for item in payload["manifest_keys"]]
    for entry in manifest_entries:
        assert entry.manifest_key in manifest_keys, "Manifest 키가 실행 입력에 포함되지 않았습니다"

    # 청크 요약 객체가 존재하는지 확인
    summary_prefix = "manifests/tmp/batch-1/"
    summary_listing = s3_client.list_objects_v2(Bucket=raw_bucket, Prefix=summary_prefix)
    assert summary_listing.get("KeyCount", 0) >= 1, "청크 요약이 S3에 저장되지 않았습니다"
