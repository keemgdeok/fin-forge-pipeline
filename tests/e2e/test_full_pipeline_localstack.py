from __future__ import annotations

import json
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Tuple

import boto3
import pytest
from botocore.exceptions import EndpointConnectionError
import runpy
import sys

from shared.clients.market_data import PriceRecord
from shared.paths import build_curated_layer_path


LOCALSTACK_ENDPOINT = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566")
LOCALSTACK_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")


def _localstack_client(service_name: str, **kwargs: Any) -> Any:
    """Return a boto3 client that targets LocalStack."""
    return boto3.client(
        service_name,
        endpoint_url=LOCALSTACK_ENDPOINT,
        region_name=kwargs.pop("region_name", LOCALSTACK_REGION),
        aws_access_key_id=kwargs.pop("aws_access_key_id", "test"),
        aws_secret_access_key=kwargs.pop("aws_secret_access_key", "test"),
        **kwargs,
    )


def _localstack_resource(service_name: str, **kwargs: Any) -> Any:
    """Return a boto3 resource that targets LocalStack."""
    return boto3.resource(
        service_name,
        endpoint_url=LOCALSTACK_ENDPOINT,
        region_name=kwargs.pop("region_name", LOCALSTACK_REGION),
        aws_access_key_id=kwargs.pop("aws_access_key_id", "test"),
        aws_secret_access_key=kwargs.pop("aws_secret_access_key", "test"),
        **kwargs,
    )


def _ensure_localstack_available() -> None:
    """Skip test execution when LocalStack is unreachable."""
    try:
        _localstack_client("s3").list_buckets()
    except EndpointConnectionError:
        pytest.skip("LocalStack endpoint not reachable; skipping e2e pipeline test")


def _patch_boto3(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force all boto3 clients/resources created by handlers to target LocalStack."""

    def _client(service_name: str, *args: Any, **kwargs: Any):
        if "endpoint_url" not in kwargs:
            kwargs["endpoint_url"] = LOCALSTACK_ENDPOINT
        if "region_name" not in kwargs:
            kwargs["region_name"] = LOCALSTACK_REGION
        if "aws_access_key_id" not in kwargs:
            kwargs["aws_access_key_id"] = "test"
        if "aws_secret_access_key" not in kwargs:
            kwargs["aws_secret_access_key"] = "test"
        return original_client(service_name, *args, **kwargs)

    def _resource(service_name: str, *args: Any, **kwargs: Any):
        if "endpoint_url" not in kwargs:
            kwargs["endpoint_url"] = LOCALSTACK_ENDPOINT
        if "region_name" not in kwargs:
            kwargs["region_name"] = LOCALSTACK_REGION
        if "aws_access_key_id" not in kwargs:
            kwargs["aws_access_key_id"] = "test"
        if "aws_secret_access_key" not in kwargs:
            kwargs["aws_secret_access_key"] = "test"
        return original_resource(service_name, *args, **kwargs)

    original_client = boto3.client
    original_resource = boto3.resource
    monkeypatch.setattr(boto3, "client", _client)
    monkeypatch.setattr(boto3, "resource", _resource)


def _ensure_load_contracts_path() -> None:
    """Ensure shared load layer is importable when runpy executes publisher handler."""
    layer_root = Path(__file__).resolve().parents[2] / "src" / "lambda" / "shared" / "layers" / "core" / "python"
    layer_str = str(layer_root)
    if layer_str not in sys.path:
        sys.path.insert(0, layer_str)


def _create_bucket(name: str) -> str:
    client = _localstack_client("s3")
    client.create_bucket(Bucket=name)
    return name


def _create_queue(name: str) -> Tuple[str, str]:
    client = _localstack_client("sqs")
    queue_url = client.create_queue(QueueName=name)["QueueUrl"]
    attrs = client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
    return queue_url, attrs["Attributes"]["QueueArn"]


def _create_batch_tracker_table(name: str) -> None:
    resource = _localstack_resource("dynamodb")
    table = resource.create_table(
        TableName=name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )
    table.wait_until_exists()


def _store_symbol_universe(param_name: str, symbols: Iterable[str]) -> None:
    client = _localstack_client("ssm")
    client.put_parameter(Name=param_name, Value=json.dumps(list(symbols)), Type="String", Overwrite=True)


def _receive_all_messages(queue_url: str) -> List[Dict[str, Any]]:
    client = _localstack_client("sqs")
    messages: List[Dict[str, Any]] = []
    while True:
        resp = client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
        batch = resp.get("Messages", [])
        if not batch:
            break
        messages.extend(batch)
    return messages


def _delete_messages(queue_url: str, messages: Iterable[Dict[str, Any]]) -> None:
    client = _localstack_client("sqs")
    for message in messages:
        client.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])


def _build_sqs_event(messages: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
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


def _parse_manifest_ds(manifest_key: str) -> str:
    match = re.search(r"year=(\d{4})/month=(\d{2})/day=(\d{2})", manifest_key)
    if not match:
        raise AssertionError(f"Unexpected manifest key format: {manifest_key}")
    year, month, day = match.groups()
    return f"{year}-{month}-{day}"


def _list_objects(bucket: str, prefix: str) -> List[str]:
    client = _localstack_client("s3")
    paginator = client.get_paginator("list_objects_v2")
    keys: List[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def _load_json_lines_objects(bucket: str, keys: Iterable[str]) -> List[Dict[str, Any]]:
    client = _localstack_client("s3")
    records: List[Dict[str, Any]] = []
    for key in keys:
        if key.endswith(".manifest.json"):
            continue
        obj = client.get_object(Bucket=bucket, Key=key)
        payload = obj["Body"].read().decode("utf-8")
        for line in payload.splitlines():
            if not line.strip():
                continue
            records.append(json.loads(line))
    return records


def _copy_objects(source_bucket: str, keys: Iterable[str], target_bucket: str, target_prefix: str) -> None:
    client = _localstack_client("s3")
    for key in keys:
        if key.endswith(".manifest.json"):
            continue
        basename = key.split("/")[-1]
        client.copy_object(
            Bucket=target_bucket,
            Key=f"{target_prefix}/{basename}",
            CopySource={"Bucket": source_bucket, "Key": key},
        )


def _write_json_object(bucket: str, key: str, payload: Dict[str, Any]) -> None:
    client = _localstack_client("s3")
    client.put_object(Bucket=bucket, Key=key, Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"))


def _stub_yahoo_finance(monkeypatch: pytest.MonkeyPatch, *, price: float = 150.0) -> None:
    from shared.clients import market_data

    class _StaticYahooClient:
        def fetch_prices(
            self,
            symbols: Iterable[str],
            period: str,
            interval: str,
        ) -> List[PriceRecord]:
            now = datetime(2025, 9, 7, tzinfo=timezone.utc)
            results: List[PriceRecord] = []
            for idx, symbol in enumerate(symbols):
                results.append(
                    PriceRecord(
                        symbol=symbol,
                        timestamp=now,
                        close=price + idx,
                        adjusted_close=price + idx + 1,
                        open=price + idx - 0.5,
                        high=price + idx + 1.5,
                        low=price + idx - 1.5,
                        volume=1_000_000 + (idx * 10_000),
                    )
                )
            return results

    monkeypatch.setattr(market_data, "YahooFinanceClient", _StaticYahooClient)


def _build_transform_input(
    *,
    manifest_key: str,
    ds: str,
    raw_bucket: str,
    domain: str,
    table_name: str,
    interval: str,
    data_source: str,
) -> Dict[str, Any]:
    return {
        "manifest_keys": [
            {
                "manifest_key": manifest_key,
                "ds": ds,
            }
        ],
        "domain": domain,
        "table_name": table_name,
        "raw_bucket": raw_bucket,
        "file_type": "json",
        "interval": interval,
        "data_source": data_source,
        "catalog_update": "force",
    }


def _build_compaction_args(glue_args: Dict[str, str], compaction_layer: str) -> Dict[str, str]:
    return {
        "--raw_bucket": glue_args["--raw_bucket"],
        "--raw_prefix": glue_args["--raw_prefix"],
        "--compacted_bucket": glue_args["--compacted_bucket"],
        "--file_type": glue_args["--file_type"],
        "--interval": glue_args["--interval"],
        "--data_source": glue_args["--data_source"],
        "--ds": glue_args["--ds"],
        "--domain": glue_args["--domain"],
        "--table_name": glue_args["--table_name"],
        "--layer": compaction_layer,
        "--codec": glue_args.get("--codec", "zstd"),
        "--target_file_mb": glue_args.get("--target_file_mb", "256"),
    }


def _run_compaction_stub(
    *,
    glue_args: Dict[str, str],
    compaction_args: Dict[str, str],
) -> str:
    ds = glue_args["--ds"]
    domain = glue_args["--domain"]
    table_name = glue_args["--table_name"]
    interval = glue_args["--interval"]
    data_source = glue_args["--data_source"]
    raw_bucket = glue_args["--raw_bucket"]
    raw_prefix_base = glue_args["--raw_prefix"].rstrip("/")

    raw_day_prefix = f"{raw_prefix_base}/year={ds[0:4]}/month={ds[5:7]}/day={ds[8:10]}/"

    raw_keys = _list_objects(raw_bucket, raw_day_prefix)
    compacted_bucket = compaction_args["--compacted_bucket"]
    compacted_prefix = build_curated_layer_path(
        domain=domain,
        table=table_name,
        interval=interval,
        data_source=data_source,
        ds=ds,
        layer=compaction_args["--layer"],
    )
    _copy_objects(raw_bucket, raw_keys, compacted_bucket, compacted_prefix)
    return compacted_prefix


def _run_etl_stub(
    *,
    glue_args: Dict[str, str],
    curated_bucket: str,
    artifacts_bucket: str,
    compacted_prefix: str,
) -> Tuple[str, Dict[str, Any], str]:
    ds = glue_args["--ds"]
    domain = glue_args["--domain"]
    table_name = glue_args["--table_name"]
    interval = glue_args["--interval"]
    data_source = glue_args["--data_source"]
    curated_layer = glue_args["--curated_layer"]

    compacted_objects = _list_objects(curated_bucket, compacted_prefix)
    records = _load_json_lines_objects(curated_bucket, compacted_objects)

    curated_prefix = build_curated_layer_path(
        domain=domain,
        table=table_name,
        interval=interval,
        data_source=data_source,
        ds=ds,
        layer=curated_layer,
    )

    summary: Dict[str, Any] = {
        "domain": domain,
        "table_name": table_name,
        "ds": ds,
        "record_count": len(records),
        "symbols": sorted({r.get("symbol") for r in records}),
    }

    parquet_key = f"{curated_prefix}/part-0000.parquet"
    _localstack_client("s3").put_object(
        Bucket=curated_bucket,
        Key=parquet_key,
        Body=b"PARQUET_PLACEHOLDER" * 128,
        ContentType="application/octet-stream",
    )

    summary_key = f"{curated_prefix}/dataset.json"
    _write_json_object(curated_bucket, summary_key, summary)

    schema_payload = {"hash": str(uuid.uuid4()), "fields": sorted({k for rec in records for k in rec.keys()})}
    fingerprint_key = f"{domain}/{table_name}/_schema/latest.json"
    _write_json_object(artifacts_bucket, fingerprint_key, schema_payload)

    return curated_prefix, summary, parquet_key


def _run_indicator_stub(
    *,
    glue_args: Dict[str, str],
    curated_bucket: str,
    curated_summary: Dict[str, Any],
) -> str:
    ds = glue_args["--ds"]
    domain = glue_args["--domain"]
    table_name = glue_args["--table_name"]
    interval = glue_args["--interval"]
    data_source = glue_args["--data_source"]
    layer = glue_args["--output_layer"]
    indicator_prefix = build_curated_layer_path(
        domain=domain,
        table=table_name,
        interval=interval,
        data_source=data_source,
        ds=ds,
        layer=layer,
    )

    payload = {
        "ds": ds,
        "indicator": "SIMPLE_MOVING_AVERAGE",
        "symbols": curated_summary.get("symbols", []),
    }
    output_key = f"{indicator_prefix}/indicators.json"
    _write_json_object(curated_bucket, output_key, payload)
    return indicator_prefix


def _invoke_preflight(
    handler: Callable[[Dict[str, Any], Any], Dict[str, Any]],
    *,
    manifest_key: str,
    ds: str,
    execution_input: Dict[str, Any],
) -> Dict[str, Any]:
    event = {
        "ds": ds,
        "domain": execution_input["domain"],
        "table_name": execution_input["table_name"],
        "file_type": execution_input["file_type"],
        "interval": execution_input["interval"],
        "data_source": execution_input["data_source"],
        "catalog_update": execution_input.get("catalog_update"),
        "source_bucket": execution_input["raw_bucket"],
        "source_key": manifest_key,
    }
    return handler(event, None)


def _build_compaction_guard_event(
    *,
    glue_args: Dict[str, str],
    compaction_layer: str,
    ds: str,
) -> Dict[str, Any]:
    return {
        "bucket": glue_args["--compacted_bucket"],
        "domain": glue_args["--domain"],
        "table_name": glue_args["--table_name"],
        "interval": glue_args["--interval"],
        "data_source": glue_args["--data_source"],
        "layer": compaction_layer,
        "ds": ds,
    }


@pytest.mark.e2e
def test_full_pipeline_end_to_end_with_localstack(monkeypatch: pytest.MonkeyPatch) -> None:
    """Execute extract → transform → load pipeline against LocalStack services."""
    _ensure_localstack_available()
    _patch_boto3(monkeypatch)
    _stub_yahoo_finance(monkeypatch)

    monkeypatch.setenv("AWS_REGION", LOCALSTACK_REGION)
    monkeypatch.setenv("AWS_DEFAULT_REGION", LOCALSTACK_REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")

    domain = "market"
    table_name = "prices"
    interval = "1d"
    data_source = "yahoo_finance"

    raw_bucket = f"e2e-raw-{uuid.uuid4().hex}"
    curated_bucket = f"e2e-curated-{uuid.uuid4().hex}"
    artifacts_bucket = f"e2e-artifacts-{uuid.uuid4().hex}"
    batch_table = f"e2e-batch-tracker-{uuid.uuid4().hex}"
    symbols_param = f"/e2e/symbols/{uuid.uuid4().hex}"

    _create_bucket(raw_bucket)
    _create_bucket(curated_bucket)
    _create_bucket(artifacts_bucket)
    _create_batch_tracker_table(batch_table)
    _store_symbol_universe(symbols_param, ["AAPL", "MSFT", "GOOGL"])

    orchestrator_queue_url, _ = _create_queue(f"e2e-orchestrator-{uuid.uuid4().hex}")
    load_queue_url, _ = _create_queue(f"e2e-load-{uuid.uuid4().hex}")

    # Extract - Orchestrator
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("QUEUE_URL", orchestrator_queue_url)
    monkeypatch.setenv("CHUNK_SIZE", "2")
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
    assert orchestrator_result["published"] == 2
    assert orchestrator_result["chunks"] == 2
    batch_id = orchestrator_result["batch_id"]
    assert batch_id

    # Extract - Worker
    worker_messages = _receive_all_messages(orchestrator_queue_url)
    assert worker_messages, "Expected orchestrator to enqueue worker messages"
    worker_event = _build_sqs_event(worker_messages)

    monkeypatch.setenv("RAW_BUCKET", raw_bucket)
    monkeypatch.setenv("ENABLE_GZIP", "false")
    monkeypatch.setenv("BATCH_TRACKING_TABLE", batch_table)
    monkeypatch.setenv("RAW_MANIFEST_BASENAME", "_batch")
    monkeypatch.setenv("RAW_MANIFEST_SUFFIX", ".manifest.json")
    worker_module = runpy.run_path("src/lambda/functions/ingestion_worker/handler.py")
    worker_result = worker_module["main"](worker_event, None)
    assert worker_result["batchItemFailures"] == []

    _delete_messages(orchestrator_queue_url, worker_messages)

    # Inspect RAW outputs and manifest
    raw_prefix = f"{domain}/{table_name}/"
    raw_objects = _list_objects(raw_bucket, raw_prefix)
    manifest_keys = [key for key in raw_objects if key.endswith(".manifest.json")]
    assert manifest_keys, "Worker should emit manifest files"
    manifest_key = manifest_keys[0]
    ds = _parse_manifest_ds(manifest_key)

    execution_input = _build_transform_input(
        manifest_key=manifest_key,
        ds=ds,
        raw_bucket=raw_bucket,
        domain=domain,
        table_name=table_name,
        interval=interval,
        data_source=data_source,
    )

    # Transform - Preflight
    monkeypatch.setenv("CURATED_BUCKET", curated_bucket)
    monkeypatch.setenv("ARTIFACTS_BUCKET", artifacts_bucket)
    monkeypatch.setenv("COMPACTION_OUTPUT_SUBDIR", "compacted")

    preflight_module = runpy.run_path("src/lambda/functions/preflight/handler.py")
    preflight_response = _invoke_preflight(
        preflight_module["lambda_handler"],
        manifest_key=manifest_key,
        ds=ds,
        execution_input=execution_input,
    )
    assert preflight_response["proceed"] is True
    glue_args = preflight_response["glue_args"]

    # Transform - Compaction
    compaction_args = _build_compaction_args(glue_args, compaction_layer="compacted")
    compacted_prefix = _run_compaction_stub(glue_args=glue_args, compaction_args=compaction_args)

    guard_event = _build_compaction_guard_event(glue_args=glue_args, compaction_layer="compacted", ds=ds)
    compaction_guard_module = runpy.run_path("src/lambda/functions/compaction_guard/handler.py")
    guard_result = compaction_guard_module["lambda_handler"](guard_event, None)
    assert guard_result["shouldProcess"] is True

    # Transform - ETL & Indicators
    curated_prefix, curated_summary, curated_parquet_key = _run_etl_stub(
        glue_args=glue_args,
        curated_bucket=curated_bucket,
        artifacts_bucket=artifacts_bucket,
        compacted_prefix=compacted_prefix,
    )

    indicators_args = {
        "--environment": glue_args["--environment"],
        "--domain": domain,
        "--table_name": table_name,
        "--interval": interval,
        "--data_source": data_source,
        "--prices_curated_bucket": curated_bucket,
        "--prices_layer": glue_args["--curated_layer"],
        "--output_bucket": curated_bucket,
        "--output_layer": "technical_indicator",
        "--schema_fingerprint_s3_uri": f"s3://{artifacts_bucket}/{domain}/{table_name}/_schema/latest.json",
        "--codec": glue_args["--codec"],
        "--target_file_mb": glue_args["--target_file_mb"],
        "--lookback_days": "30",
        "--ds": ds,
    }

    indicator_prefix = _run_indicator_stub(
        glue_args=indicators_args,
        curated_bucket=curated_bucket,
        curated_summary=curated_summary,
    )

    # Transform - Schema decider
    monkeypatch.setenv("CATALOG_UPDATE_DEFAULT", "force")
    decider_event = {"glue_args": indicators_args, "catalog_update": execution_input.get("catalog_update")}
    schema_decider_module = runpy.run_path("src/lambda/functions/schema_change_decider/handler.py")
    decider_result = schema_decider_module["lambda_handler"](decider_event, None)
    assert isinstance(decider_result["shouldRunCrawler"], bool)

    # Load - emit curated S3 event and publish to load queue
    curated_objects = _list_objects(curated_bucket, curated_prefix)
    assert curated_objects, "Curated dataset should contain output objects"
    curated_object_key = curated_parquet_key
    head_resp = _localstack_client("s3").head_object(Bucket=curated_bucket, Key=curated_object_key)
    curated_size = int(head_resp["ContentLength"])

    load_event = {
        "source": "aws.s3",
        "detail-type": "Object Created",
        "detail": {
            "bucket": {"name": curated_bucket},
            "object": {
                "key": curated_object_key,
                "size": curated_size,
            },
        },
    }

    queue_map = {domain: load_queue_url}
    monkeypatch.setenv("LOAD_QUEUE_MAP", json.dumps(queue_map))
    monkeypatch.setenv("PRIORITY_MAP", json.dumps({domain: "1"}))
    monkeypatch.setenv("MIN_FILE_SIZE_BYTES", "1")
    monkeypatch.setenv("ALLOWED_LAYERS", json.dumps(["adjusted", "technical_indicator"]))

    _ensure_load_contracts_path()
    load_publisher_module = runpy.run_path("src/lambda/functions/load_event_publisher/handler.py")
    publisher_result = load_publisher_module["main"](load_event, None)
    assert publisher_result["status"] == "SUCCESS"

    load_messages = _receive_all_messages(load_queue_url)
    assert load_messages, "Load queue should receive published message"
    message_body = json.loads(load_messages[0]["Body"])
    assert message_body["domain"] == domain
    assert message_body["table_name"] == table_name
    assert message_body["layer"] == glue_args["--curated_layer"]

    # Final assertions: ensure artifacts exist
    artifacts_keys = _list_objects(artifacts_bucket, f"{domain}/{table_name}/")
    assert any(key.endswith("_schema/latest.json") for key in artifacts_keys)
    indicator_objects = _list_objects(curated_bucket, indicator_prefix)
    assert indicator_objects, "Indicator layer should contain output files"
