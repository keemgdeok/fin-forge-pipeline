from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Iterable, List

import runpy

import pytest

from tests.e2e.utils.env import LocalStackConfig
from tests.e2e.utils import localstack_ops


@dataclass
class IngestionContext:
    domain: str
    table_name: str
    interval: str
    data_source: str
    indicator_layer: str
    raw_bucket: str
    curated_bucket: str
    artifacts_bucket: str
    batch_table: str
    symbols_param: str
    orchestrator_queue_url: str
    load_queue_url: str
    manifest_keys: List[str]


def run_ingestion(
    monkeypatch: pytest.MonkeyPatch,
    config: LocalStackConfig,
    *,
    domain: str,
    table_name: str,
    interval: str,
    data_source: str,
    indicator_layer: str,
    symbols: Iterable[str],
    manifest_days: int,
    chunk_size: int = 10,
) -> IngestionContext:
    """Run orchestrator and worker Lambdas to populate RAW data and manifests."""
    raw_bucket = f"e2e-raw-{uuid.uuid4().hex}"
    curated_bucket = f"e2e-curated-{uuid.uuid4().hex}"
    artifacts_bucket = f"e2e-artifacts-{uuid.uuid4().hex}"
    batch_table = f"e2e-batch-tracker-{uuid.uuid4().hex}"
    symbols_param = f"/e2e/symbols/{uuid.uuid4().hex}"

    localstack_ops.create_bucket(config, raw_bucket)
    localstack_ops.create_bucket(config, curated_bucket)
    localstack_ops.create_bucket(config, artifacts_bucket)
    localstack_ops.create_batch_tracker_table(config, batch_table)
    localstack_ops.store_symbol_universe(config, symbols_param, symbols)

    orchestrator_queue_url, _ = localstack_ops.create_queue(config, f"e2e-orchestrator-{uuid.uuid4().hex}")
    load_queue_url, _ = localstack_ops.create_queue(config, f"e2e-load-{uuid.uuid4().hex}")

    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("QUEUE_URL", orchestrator_queue_url)
    monkeypatch.setenv("CHUNK_SIZE", str(chunk_size))
    monkeypatch.setenv("SQS_SEND_BATCH_SIZE", "10")
    monkeypatch.setenv("BATCH_TRACKING_TABLE", batch_table)
    monkeypatch.setenv("BATCH_TRACKER_TTL_DAYS", "7")
    monkeypatch.setenv("SYMBOLS_SSM_PARAM", symbols_param)
    monkeypatch.delenv("SYMBOLS_S3_BUCKET", raising=False)
    monkeypatch.delenv("SYMBOLS_S3_KEY", raising=False)

    orchestrator_event = {
        "domain": domain,
        "table_name": table_name,
        "symbols": [],
        "period": "1mo",
        "interval": interval,
        "file_format": "json",
        "data_source": data_source,
        "data_type": "prices",
    }

    orchestrator_module = runpy.run_path("src/lambda/functions/ingestion_orchestrator/handler.py")
    orchestrator_result = orchestrator_module["main"](orchestrator_event, None)
    assert orchestrator_result["published"] >= 1

    worker_messages = localstack_ops.receive_all_messages(config, orchestrator_queue_url)
    worker_event = localstack_ops.build_sqs_event(worker_messages)

    monkeypatch.setenv("RAW_BUCKET", raw_bucket)
    monkeypatch.setenv("ENABLE_GZIP", "false")
    monkeypatch.setenv("BATCH_TRACKING_TABLE", batch_table)
    monkeypatch.setenv("RAW_MANIFEST_BASENAME", "_batch")
    monkeypatch.setenv("RAW_MANIFEST_SUFFIX", ".manifest.json")

    worker_module = runpy.run_path("src/lambda/functions/ingestion_worker/handler.py")
    worker_result = worker_module["main"](worker_event, None)
    assert worker_result["batchItemFailures"] == []
    localstack_ops.delete_messages(config, orchestrator_queue_url, worker_messages)

    raw_prefix = f"{domain}/{table_name}/"
    manifest_keys = sorted(
        key for key in localstack_ops.list_objects(config, raw_bucket, raw_prefix) if key.endswith(".manifest.json")
    )

    assert len(manifest_keys) == manifest_days, f"Expected {manifest_days} manifest files, got {len(manifest_keys)}"

    context = IngestionContext(
        domain=domain,
        table_name=table_name,
        interval=interval,
        data_source=data_source,
        indicator_layer=indicator_layer,
        raw_bucket=raw_bucket,
        curated_bucket=curated_bucket,
        artifacts_bucket=artifacts_bucket,
        batch_table=batch_table,
        symbols_param=symbols_param,
        orchestrator_queue_url=orchestrator_queue_url,
        load_queue_url=load_queue_url,
        manifest_keys=manifest_keys,
    )

    return context
