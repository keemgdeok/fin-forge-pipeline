"""Typed helper to start manifest-driven processing executions.

Example
-------

from src.step_functions.workflows.runner import (
    ManifestItem,
    TransformExecutionInput,
    start_transform_execution,
)

payload = TransformExecutionInput(
    manifest_items=[
        ManifestItem(
            ds="2025-09-10",
            manifest_key="market/prices/interval=1d/data_source=yahoo_finance/year=2025/month=09/day=10/_batch.manifest.json",
        )
    ],
    domain="market",
    table_name="prices",
    raw_bucket="data-pipeline-raw-dev-123456789012",
    environment="dev",
)

execution_arn = start_transform_execution(
    sm_arn="arn:aws:states:ap-northeast-2:123456789012:stateMachine:dev-daily-prices-data-processing",
    payload=payload,
)
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import boto3


@dataclass
class ManifestItem:
    """Single manifest entry to be processed by the state machine."""

    ds: str
    manifest_key: str
    source: Optional[str] = None

    def to_dict(self) -> Dict[str, str]:
        payload = {"ds": self.ds, "manifest_key": self.manifest_key}
        if self.source:
            payload["source"] = self.source
        return payload


@dataclass
class TransformExecutionInput:
    """Input payload for the manifest-driven processing workflow."""

    manifest_items: List[ManifestItem]
    domain: str
    table_name: str
    raw_bucket: str
    file_type: str = "json"
    interval: str = "1d"
    data_source: str = "yahoo_finance"
    catalog_update: Optional[str] = None
    environment: Optional[str] = None
    batch_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        if not self.manifest_items:
            raise ValueError("Provide at least one manifest item")

        payload: Dict[str, Any] = {
            "manifest_keys": [item.to_dict() for item in self.manifest_items],
            "domain": self.domain,
            "table_name": self.table_name,
            "file_type": self.file_type,
            "interval": self.interval,
            "data_source": self.data_source,
            "raw_bucket": self.raw_bucket,
        }

        if self.catalog_update:
            payload["catalog_update"] = self.catalog_update
        if self.environment:
            payload["environment"] = self.environment
        if self.batch_id:
            payload["batch_id"] = self.batch_id

        return payload


def start_transform_execution(
    *,
    sm_arn: str,
    payload: TransformExecutionInput,
    region_name: Optional[str] = None,
    name: Optional[str] = None,
) -> str:
    """Start the transform state machine execution and return the execution ARN.

    Validates that at least one manifest entry is supplied and forwards the
    manifest list plus domain/table metadata to Step Functions.
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
    if not payload.manifest_items:
        raise ValueError("Provide at least one manifest item")
    if not payload.raw_bucket:
        raise ValueError("raw_bucket is required for manifest-driven executions")
