#!/usr/bin/env python3
"""Run end-to-end validation for the daily prices pipeline after deployment.

Steps
-----
1. Capture baseline SQS queue depth so we can confirm new load messages arrive.
2. Invoke the ingestion orchestrator Lambda to fan-out symbols into the worker queue.
3. Wait for the DynamoDB batch tracker entry to report `status=complete` (extract done).
4. Locate the Step Functions execution that EventBridge launched and wait for success.
5. Verify recent Glue job runs (compaction, ETL, indicators) completed successfully.
6. Check curated S3 prefixes for the processed ds and confirm indicator outputs exist.
7. Re-read SQS queue depth to ensure at least one new message is queued for on-prem load
   while the DLQ remains empty, then emit a machine-readable summary for the workflow.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from infrastructure.config.environments import get_environment_config


@dataclass
class QueueSnapshot:
    visible: int
    not_visible: int
    delayed: int


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _read_queue_metrics(sqs_client, queue_url: str) -> QueueSnapshot:
    resp = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed",
        ],
    )
    attrs = resp.get("Attributes", {})
    return QueueSnapshot(
        visible=int(attrs.get("ApproximateNumberOfMessages", "0")),
        not_visible=int(attrs.get("ApproximateNumberOfMessagesNotVisible", "0")),
        delayed=int(attrs.get("ApproximateNumberOfMessagesDelayed", "0")),
    )


def _wait_for_batch_completion(table, batch_id: str, timeout: int, interval: int) -> Dict[str, Any]:
    deadline = _now() + timedelta(seconds=timeout)
    last_item: Dict[str, Any] | None = None
    while _now() < deadline:
        try:
            resp = table.get_item(Key={"pk": batch_id})
        except ClientError as exc:  # pragma: no cover - surfaced via raise
            raise RuntimeError(f"Failed to read batch tracker: {exc}") from exc
        item = resp.get("Item")
        if item:
            last_item = item
            status = item.get("status")
            if status == "complete":
                return item
            if status == "error":
                msg = item.get("error_message", "unknown error")
                raise RuntimeError(f"Batch tracker reported error: {msg}")
        time.sleep(interval)
    raise TimeoutError(
        "Timed out waiting for ingestion batch to complete"
        if not last_item
        else f"Timed out waiting for batch status to reach 'complete' (last: {last_item.get('status')})"
    )


def _find_recent_execution(
    sfn_client, state_machine_arn: str, started_after: datetime, timeout: int, interval: int
) -> Dict[str, Any]:
    tolerance = timedelta(minutes=5)
    deadline = _now() + timedelta(seconds=timeout)
    candidate: Optional[Dict[str, Any]] = None
    while _now() < deadline:
        resp = sfn_client.list_executions(stateMachineArn=state_machine_arn, maxResults=10)
        for execution in resp.get("executions", []):
            start_time: datetime = execution.get("startDate")
            if start_time and start_time + tolerance >= started_after:
                candidate = execution
                break
        if candidate:
            return candidate
        time.sleep(interval)
    raise TimeoutError("Unable to locate Step Functions execution started after ingestion run")


def _await_execution_success(sfn_client, execution_arn: str, timeout: int, interval: int) -> Dict[str, Any]:
    deadline = _now() + timedelta(seconds=timeout)
    while _now() < deadline:
        desc = sfn_client.describe_execution(executionArn=execution_arn)
        status = desc.get("status")
        if status == "SUCCEEDED":
            return desc
        if status in {"FAILED", "TIMED_OUT", "ABORTED"}:
            cause = desc.get("cause") or desc.get("error") or "unknown"
            raise RuntimeError(f"State machine execution failed ({status}): {cause}")
        time.sleep(interval)
    raise TimeoutError("Timed out waiting for Step Functions execution to finish")


def _latest_job_run(glue_client, job_name: str, after: datetime) -> Dict[str, Any]:
    resp = glue_client.get_job_runs(JobName=job_name, MaxResults=20)
    for job_run in resp.get("JobRuns", []):
        started_on: Optional[datetime] = job_run.get("StartedOn")
        if started_on and started_on >= after:
            return job_run
    raise RuntimeError(f"No Glue job runs found for {job_name} after {after.isoformat()}")


def _prefix_has_objects(s3_client, bucket: str, prefix: str) -> bool:
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return int(resp.get("KeyCount", 0)) > 0


def _wait_for_queue_growth(
    sqs_client,
    queue_url: str,
    baseline: int,
    minimum_increase: int,
    timeout: int,
    interval: int,
) -> QueueSnapshot:
    deadline = _now() + timedelta(seconds=timeout)
    while _now() < deadline:
        snapshot = _read_queue_metrics(sqs_client, queue_url)
        if snapshot.visible >= baseline + minimum_increase:
            return snapshot
        time.sleep(interval)
    raise TimeoutError("Expected load queue to accumulate new messages but threshold not reached")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate extract/transform/load pipeline post-deploy")
    parser.add_argument("--environment", "-e", default="dev", help="Target environment (dev|staging|prod)")
    parser.add_argument("--ingestion-timeout", type=int, default=900, help="Seconds to wait for ingestion completion")
    parser.add_argument(
        "--execution-timeout", type=int, default=1800, help="Seconds to wait for Step Functions success"
    )
    parser.add_argument("--queue-timeout", type=int, default=600, help="Seconds to wait for load queue growth")
    parser.add_argument(
        "--expected-visible-increase",
        type=int,
        default=1,
        help="Minimum additional visible messages expected on the load queue",
    )
    parser.add_argument("--output-json", default="pipeline_validation_summary.json", help="Summary JSON output path")
    parser.add_argument("--output-text", default="pipeline_validation_summary.txt", help="Summary text output path")
    args = parser.parse_args()

    session = boto3.Session()
    region = session.region_name or session.client("sts").meta.region_name or "ap-northeast-2"
    sts_client = session.client("sts")
    account_id = sts_client.get_caller_identity()["Account"]

    config = get_environment_config(args.environment)
    domain = str(config.get("ingestion_domain", "market"))
    table_name = str(config.get("ingestion_table_name", "prices"))
    indicators_table = str(config.get("indicators_table_name", "indicators"))
    period = str(config.get("ingestion_period", "1mo"))
    interval_value = str(config.get("ingestion_interval", "1d"))
    file_format = str(config.get("ingestion_file_format", "json"))
    symbols = list(config.get("ingestion_symbols", ["AAPL", "MSFT"]))

    load_configs = list(config.get("load_domain_configs", []))
    if not load_configs:
        raise RuntimeError("Environment config does not define load_domain_configs")
    load_domain = str(load_configs[0].get("domain", domain))

    lambda_client = session.client("lambda")
    dynamodb = session.resource("dynamodb")
    sfn_client = session.client("stepfunctions")
    glue_client = session.client("glue")
    sqs_client = session.client("sqs")
    s3_client = session.client("s3")

    orchestrator_function = f"{args.environment}-daily-prices-data-orchestrator"
    batch_table_name = (
        config.get("batch_tracker_table_name") or f"{args.environment}-daily-prices-batch-tracker"
    ).strip()
    batch_tracker_table = dynamodb.Table(batch_table_name)

    state_machine_arn = (
        f"arn:aws:states:{region}:{account_id}:stateMachine:{args.environment}-daily-prices-data-processing"
    )
    compaction_job = f"{args.environment}-daily-prices-compaction"
    etl_job = f"{args.environment}-daily-prices-data-etl"
    indicators_job = f"{args.environment}-market-indicators-etl"

    curated_bucket = f"data-pipeline-curated-{args.environment}-{account_id}"

    queue_name = f"{args.environment}-{load_domain}-load-queue"
    dlq_name = f"{args.environment}-{load_domain}-load-dlq"

    try:
        queue_url = sqs_client.get_queue_url(QueueName=queue_name)["QueueUrl"]
        dlq_url = sqs_client.get_queue_url(QueueName=dlq_name)["QueueUrl"]
    except ClientError as exc:  # pragma: no cover - infrastructure issue
        raise RuntimeError(f"Unable to resolve load queue URLs: {exc}") from exc

    pre_queue = _read_queue_metrics(sqs_client, queue_url)
    pre_dlq = _read_queue_metrics(sqs_client, dlq_url)

    batch_id = f"ci-{uuid.uuid4()}"
    batch_ds = _now().date().isoformat()
    ingest_event: Dict[str, Any] = {
        "data_source": "yahoo_finance",
        "data_type": "prices",
        "domain": domain,
        "table_name": table_name,
        "symbols": symbols,
        "period": period,
        "interval": interval_value,
        "file_format": file_format,
        "trigger_type": "manual",
        "batch_id": batch_id,
        "batch_ds": batch_ds,
    }

    print(f"Invoking orchestrator Lambda {orchestrator_function} for batch {batch_id}...")
    try:
        response = lambda_client.invoke(
            FunctionName=orchestrator_function,
            InvocationType="RequestResponse",
            Payload=json.dumps(ingest_event).encode("utf-8"),
        )
    except (BotoCoreError, ClientError) as exc:  # pragma: no cover - AWS failure path
        raise RuntimeError(f"Failed to invoke orchestrator Lambda: {exc}") from exc

    payload_bytes = response.get("Payload").read()
    result = json.loads(payload_bytes or b"{}")
    if response.get("FunctionError"):
        raise RuntimeError(f"Orchestrator Lambda reported error: {result}")

    published = int(result.get("published", 0))
    chunks = int(result.get("chunks", 0))
    ingestion_start = _now()
    print(f"Published {published} chunk(s) across {chunks} batch entries")

    print("Waiting for DynamoDB batch tracker to reach 'complete'...")
    batch_record = _wait_for_batch_completion(
        batch_tracker_table,
        batch_id=batch_id,
        timeout=args.ingestion_timeout,
        interval=10,
    )
    manifest_keys = batch_record.get("manifest_keys") or []
    print(f"Batch tracker status: {batch_record.get('status')} (manifest keys: {len(manifest_keys)})")

    print("Locating Step Functions execution launched by EventBridge...")
    execution_stub = _find_recent_execution(
        sfn_client,
        state_machine_arn=state_machine_arn,
        started_after=ingestion_start - timedelta(minutes=1),
        timeout=args.execution_timeout,
        interval=15,
    )
    execution_arn = execution_stub["executionArn"]
    execution_start: datetime = execution_stub["startDate"]
    print(f"Found execution {execution_arn}")

    print("Waiting for Step Functions execution to succeed...")
    execution_detail = _await_execution_success(
        sfn_client,
        execution_arn=execution_arn,
        timeout=args.execution_timeout,
        interval=15,
    )
    execution_stop: datetime = execution_detail.get("stopDate", _now())

    print("Validating Glue job runs...")
    compaction_run = _latest_job_run(glue_client, compaction_job, execution_start)
    etl_run = _latest_job_run(glue_client, etl_job, execution_start)
    indicators_run = _latest_job_run(glue_client, indicators_job, execution_start)
    for name, run in (
        ("compaction", compaction_run),
        ("etl", etl_run),
        ("indicators", indicators_run),
    ):
        state = run.get("JobRunState")
        if state != "SUCCEEDED":
            raise RuntimeError(f"Glue {name} job run did not succeed (state={state})")

    curated_prefix = f"{domain}/{table_name}/adjusted/ds={batch_ds}/"
    indicators_prefix = f"{domain}/{table_name}/{indicators_table}/ds={batch_ds}/"
    curated_ready = _prefix_has_objects(s3_client, curated_bucket, curated_prefix)
    indicators_ready = _prefix_has_objects(s3_client, curated_bucket, indicators_prefix)
    if not curated_ready:
        raise RuntimeError(f"Curated data prefix missing objects: s3://{curated_bucket}/{curated_prefix}")
    if not indicators_ready:
        raise RuntimeError(f"Indicators prefix missing objects: s3://{curated_bucket}/{indicators_prefix}")

    print("Waiting for load queue to accumulate new messages...")
    post_queue = _wait_for_queue_growth(
        sqs_client,
        queue_url=queue_url,
        baseline=pre_queue.visible,
        minimum_increase=args.expected_visible_increase,
        timeout=args.queue_timeout,
        interval=15,
    )
    post_dlq = _read_queue_metrics(sqs_client, dlq_url)
    if post_dlq.visible > pre_dlq.visible:
        raise RuntimeError("Load DLQ received messages; investigate before releasing")

    summary: Dict[str, Any] = {
        "environment": args.environment,
        "region": region,
        "batch_id": batch_id,
        "batch_ds": batch_ds,
        "ingestion": {
            "published_messages": published,
            "chunks": chunks,
            "manifest_keys": manifest_keys,
            "tracker_status": batch_record.get("status"),
        },
        "step_function": {
            "execution_arn": execution_arn,
            "start_time": execution_start.isoformat(),
            "stop_time": execution_stop.isoformat(),
            "duration_seconds": (execution_stop - execution_start).total_seconds(),
        },
        "glue_jobs": {
            "compaction": {"run_id": compaction_run.get("Id"), "state": compaction_run.get("JobRunState")},
            "etl": {"run_id": etl_run.get("Id"), "state": etl_run.get("JobRunState")},
            "indicators": {"run_id": indicators_run.get("Id"), "state": indicators_run.get("JobRunState")},
        },
        "s3_checks": {
            "curated_prefix": f"s3://{curated_bucket}/{curated_prefix}",
            "indicators_prefix": f"s3://{curated_bucket}/{indicators_prefix}",
            "curated_has_objects": curated_ready,
            "indicators_has_objects": indicators_ready,
        },
        "queue_metrics": {
            "queue_name": queue_name,
            "pre_visible": pre_queue.visible,
            "post_visible": post_queue.visible,
            "post_not_visible": post_queue.not_visible,
            "dlq_name": dlq_name,
            "dlq_visible": post_dlq.visible,
        },
    }

    json_path = Path(args.output_json)
    json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    lines = [
        f"Environment: {args.environment}",
        f"Batch ID: {batch_id}",
        f"Batch date: {batch_ds}",
        f"Ingestion chunks: {chunks} (messages published: {published})",
        f"State machine: {execution_arn}",
        "Glue runs:",
        f"  compaction={compaction_run.get('Id')}",
        f"  etl={etl_run.get('Id')}",
        f"  indicators={indicators_run.get('Id')}",
        f"Curated data prefix: s3://{curated_bucket}/{curated_prefix}",
        f"Indicators prefix: s3://{curated_bucket}/{indicators_prefix}",
        f"Load queue visible messages: {pre_queue.visible} -> {post_queue.visible}",
        f"Load DLQ visible messages: {pre_dlq.visible} -> {post_dlq.visible}",
    ]
    text_path = Path(args.output_text)
    text_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print("\n".join(lines))
    print(f"Summary written to {json_path} and {text_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover - ensures clean exit for CI
        print(f"Pipeline validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
