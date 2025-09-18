"""Typed helper to start Transform Step Functions executions.

This utility provides a minimal, type-safe interface to trigger the
transform pipeline state machine, following the documented contract.

Usage example:

from src.step_functions.workflows.runner import (
    TransformExecutionInput,
    start_transform_execution,
)

payload = TransformExecutionInput(
    environment="dev",
    domain="market",
    table_name="prices",
    source_bucket="data-pipeline-raw-dev-1234",
    source_key="market/prices/ingestion_date=2025-09-07/file.json",
    file_type="json",
)

execution_arn = start_transform_execution(
    sm_arn="arn:aws:states:us-east-1:123456789012:stateMachine:dev-daily-prices-data-processing",
    payload=payload,
    region_name="us-east-1",
)
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional

import boto3


@dataclass
class DateRange:
    start: str
    end: str

    def to_dict(self) -> Dict[str, str]:
        return {"start": self.start, "end": self.end}


@dataclass
class TransformExecutionInput:
    """Input payload for the transform state machine.

    Supports both direct mode (ds or date_range) and S3 trigger mode
    (source_bucket/source_key) per the state machine contract.
    """

    environment: str
    domain: str
    table_name: str

    # Direct mode (XOR with date_range). Either `ds` or `date_range`.
    ds: Optional[str] = None
    date_range: Optional[DateRange] = None

    # S3 trigger mode
    source_bucket: Optional[str] = None
    source_key: Optional[str] = None
    file_type: str = "json"

    # Optional ancillary fields
    reprocess: Optional[bool] = None
    execution_id: Optional[str] = None
    catalog_update: Optional[str] = None  # on_schema_change|never|force

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "environment": self.environment,
            "domain": self.domain,
            "table_name": self.table_name,
            "file_type": self.file_type,
        }

        if self.execution_id:
            payload["execution_id"] = self.execution_id
        if self.reprocess is not None:
            payload["reprocess"] = bool(self.reprocess)
        if self.catalog_update:
            payload["catalog_update"] = self.catalog_update

        if self.ds:
            payload["ds"] = self.ds
        elif self.date_range:
            payload["date_range"] = self.date_range.to_dict()

        if self.source_bucket and self.source_key:
            payload["source_bucket"] = self.source_bucket
            payload["source_key"] = self.source_key

        return payload


def start_transform_execution(
    *,
    sm_arn: str,
    payload: TransformExecutionInput,
    region_name: Optional[str] = None,
    name: Optional[str] = None,
) -> str:
    """Start the transform state machine execution and return the execution ARN.

    - Validates that either ds/date_range or S3 trigger inputs are provided.
    - Uses `boto3.client('stepfunctions')` to invoke StartExecution.
    """
    _validate_payload(payload)

    client = boto3.client("stepfunctions", region_name=region_name)
    args: Dict[str, Any] = {
        "stateMachineArn": sm_arn,
        "input": json.dumps(payload.to_dict()),
    }
    if name:
        args["name"] = name

    resp = client.start_execution(**args)
    return str(resp.get("executionArn", ""))


def _validate_payload(payload: TransformExecutionInput) -> None:
    has_direct = bool(payload.ds or payload.date_range)
    has_s3_trigger = bool(payload.source_bucket and payload.source_key)

    if not has_direct and not has_s3_trigger:
        raise ValueError("Provide either ds/date_range or source_bucket/source_key")

    if payload.date_range:
        # Simple structural validation; content validation left to Preflight
        if not payload.date_range.start or not payload.date_range.end:
            raise ValueError("date_range.start and date_range.end are required when date_range is provided")
