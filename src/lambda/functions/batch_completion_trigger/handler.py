"""DynamoDB stream trigger that starts the processing Step Functions workflow."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

from shared.ingestion.manifests import collect_manifest_entries
from shared.utils.logger import get_logger


logger = get_logger(__name__)
_DESERIALIZER = TypeDeserializer()


@dataclass(frozen=True)
class Settings:
    """Environment-derived settings for the trigger."""

    environment: str
    state_machine_arn: str
    raw_bucket: str
    tracker_table_name: str
    manifest_basename: str
    manifest_suffix: str
    catalog_update_default: str
    default_domain: str
    default_table_name: str
    default_interval: str
    default_data_source: str
    default_file_type: str


def _required_env(name: str, default: Optional[str] = None) -> str:
    value = os.environ.get(name, default)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _load_settings() -> Settings:
    return Settings(
        environment=_required_env("ENVIRONMENT", ""),
        state_machine_arn=_required_env("STATE_MACHINE_ARN"),
        raw_bucket=_required_env("RAW_BUCKET"),
        tracker_table_name=_required_env("BATCH_TRACKER_TABLE"),
        manifest_basename=_required_env("MANIFEST_BASENAME", "_batch"),
        manifest_suffix=_required_env("MANIFEST_SUFFIX", ".manifest.json"),
        catalog_update_default=_required_env("CATALOG_UPDATE_DEFAULT", "on_schema_change"),
        default_domain=_required_env("DEFAULT_DOMAIN", ""),
        default_table_name=_required_env("DEFAULT_TABLE_NAME", ""),
        default_interval=_required_env("DEFAULT_INTERVAL", "1d"),
        default_data_source=_required_env("DEFAULT_DATA_SOURCE", "yahoo_finance"),
        default_file_type=_required_env("DEFAULT_FILE_TYPE", "json"),
    )


def _deserialize_attribute(value: Optional[Dict[str, Any]]) -> Any:
    if value is None:
        return None
    return _DESERIALIZER.deserialize(value)


def _deserialize_image(image: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not image:
        return {}
    return {key: _deserialize_attribute(value) for key, value in image.items()}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _manifest_payload(entries) -> List[Dict[str, str]]:
    payload: List[Dict[str, str]] = []
    for entry in entries:
        record = {"ds": entry.ds, "manifest_key": entry.manifest_key}
        if entry.source:
            record["source"] = entry.source
        payload.append(record)
    return payload


def _lock_batch(table, batch_id: str) -> bool:
    """Acquire an idempotent lock to avoid duplicate workflow starts."""
    try:
        table.update_item(
            Key={"pk": batch_id},
            UpdateExpression=("SET processing_execution_status = :status, processing_execution_locked_at = :now"),
            ConditionExpression=(
                "attribute_not_exists(processing_execution_status) AND attribute_not_exists(processing_execution_arn)"
            ),
            ExpressionAttributeValues={
                ":status": "starting",
                ":now": _now_iso(),
            },
        )
        return True
    except ClientError as exc:  # pragma: no cover - defensive logging path
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code == "ConditionalCheckFailedException":
            logger.info("Processing already started for batch", extra={"batch_id": batch_id})
            return False
        logger.exception("Failed to acquire processing lock", extra={"batch_id": batch_id})
        raise


def _mark_execution_started(table, batch_id: str, execution_arn: str) -> None:
    try:
        table.update_item(
            Key={"pk": batch_id},
            UpdateExpression=(
                "SET processing_execution_arn = :arn, "
                "processing_execution_started_at = :now, "
                "processing_execution_status = :status "
                "REMOVE processing_execution_locked_at"
            ),
            ConditionExpression="processing_execution_status = :expected",
            ExpressionAttributeValues={
                ":arn": execution_arn,
                ":now": _now_iso(),
                ":status": "started",
                ":expected": "starting",
            },
        )
    except ClientError as exc:  # pragma: no cover - defensive logging path
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code != "ConditionalCheckFailedException":
            logger.exception(
                "Failed to record processing execution start",
                extra={"batch_id": batch_id, "execution_arn": execution_arn},
            )


def _reset_lock_on_failure(table, batch_id: str, reason: str) -> None:
    try:
        table.update_item(
            Key={"pk": batch_id},
            UpdateExpression=(
                "REMOVE processing_execution_status, processing_execution_locked_at "
                "SET processing_execution_error = :reason, processing_execution_error_at = :now"
            ),
            ExpressionAttributeValues={
                ":reason": reason,
                ":now": _now_iso(),
            },
        )
    except ClientError:  # pragma: no cover - defensive logging path
        logger.exception(
            "Failed to reset processing lock after error",
            extra={"batch_id": batch_id, "reason": reason},
        )


def _process_record(
    record: Dict[str, Any],
    settings: Settings,
    *,
    table,
    stepfunctions_client,
) -> bool:
    if record.get("eventName") != "MODIFY":
        return False

    dynamodb_record = record.get("dynamodb") or {}
    new_image = _deserialize_image(dynamodb_record.get("NewImage"))
    if not new_image:
        return False

    old_image = _deserialize_image(dynamodb_record.get("OldImage"))

    new_status = str(new_image.get("status") or "")
    old_status = str(old_image.get("status") or "")

    if new_status.lower() != "complete" or old_status.lower() == "complete":
        return False

    if new_image.get("processing_execution_arn"):
        logger.info("Batch already linked to a processing execution", extra={"batch_id": new_image.get("pk")})
        return False

    batch_id = str(new_image.get("pk") or "")
    if not batch_id:
        logger.warning("Missing batch identifier in stream record", extra={"record": record})
        return False

    if not _lock_batch(table, batch_id):
        return False

    domain = str(new_image.get("meta_domain") or settings.default_domain or "market")
    table_name = str(new_image.get("meta_table_name") or settings.default_table_name or "prices")
    interval = str(new_image.get("meta_interval") or settings.default_interval or "1d")
    data_source = str(new_image.get("meta_data_source") or settings.default_data_source or "yahoo_finance")
    file_type = str(new_image.get("meta_file_format") or settings.default_file_type or "json")
    catalog_update = str(new_image.get("catalog_update") or settings.catalog_update_default or "on_schema_change")
    environment = str(new_image.get("meta_environment") or settings.environment or "dev")

    manifest_entries = collect_manifest_entries(
        batch_id=batch_id,
        raw_bucket=settings.raw_bucket,
        domain=domain,
        table_name=table_name,
        interval=interval,
        data_source=data_source,
        manifest_basename=settings.manifest_basename,
        manifest_suffix=settings.manifest_suffix,
        tracker_table=table,
        tracker_item=new_image,
    )

    if not manifest_entries:
        logger.warning("No manifest entries discovered for batch", extra={"batch_id": batch_id})
        _reset_lock_on_failure(table, batch_id, "NoManifestEntries")
        return False

    execution_input = {
        "manifest_keys": _manifest_payload(manifest_entries),
        "domain": domain,
        "table_name": table_name,
        "file_type": file_type,
        "interval": interval,
        "data_source": data_source,
        "raw_bucket": settings.raw_bucket,
        "catalog_update": catalog_update,
        "batch_id": batch_id,
        "environment": environment,
    }

    try:
        response = stepfunctions_client.start_execution(
            stateMachineArn=settings.state_machine_arn,
            input=json.dumps(execution_input),
        )
    except ClientError:
        logger.exception("Failed to start processing workflow", extra={"batch_id": batch_id})
        _reset_lock_on_failure(table, batch_id, "StartExecutionFailed")
        raise

    execution_arn = str(response.get("executionArn") or "")
    if not execution_arn:
        logger.error("Step Functions execution ARN missing in response", extra={"batch_id": batch_id})
        _reset_lock_on_failure(table, batch_id, "MissingExecutionArn")
        raise RuntimeError("Step Functions execution ARN missing")

    _mark_execution_started(table, batch_id, execution_arn)

    logger.info(
        "Triggered processing workflow",
        extra={
            "batch_id": batch_id,
            "execution_arn": execution_arn,
            "manifest_count": len(execution_input["manifest_keys"]),
        },
    )
    return True


def main(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda entry point with partial batch failure reporting."""
    settings = _load_settings()
    dynamodb_table = boto3.resource("dynamodb").Table(settings.tracker_table_name)
    stepfunctions_client = boto3.client("stepfunctions")

    processed = 0
    failures: List[Dict[str, str]] = []

    for record in event.get("Records", []):
        event_id = record.get("eventID")
        try:
            if _process_record(
                record,
                settings,
                table=dynamodb_table,
                stepfunctions_client=stepfunctions_client,
            ):
                processed += 1
        except Exception:  # pragma: no cover - surfaced via batch failure reporting
            logger.exception("Error processing DynamoDB stream record", extra={"event_id": event_id})
            if event_id:
                failures.append({"itemIdentifier": event_id})

    return {
        "processed": processed,
        "batchItemFailures": failures,
    }
