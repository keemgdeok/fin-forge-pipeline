"""Typed event models for Lambda handlers using Pydantic v2.

Includes models for ingestion, validation, notifications, and transform pipeline
events (preflight input/output). These models live in the Common Layer so both
Lambda functions and offline tooling can share a single, type-safe contract.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


class DataIngestionEvent(BaseModel):
    data_source: str = "yahoo_finance"
    data_type: str = "prices"
    symbols: List[str] = Field(default_factory=list)
    period: str = "1y"
    interval: str = "1d"
    domain: str = "market"
    table_name: str = "prices"
    file_format: str = "parquet"

    @field_validator("symbols", mode="before")
    @classmethod
    def _coerce_symbols(cls, v: Any) -> List[str]:  # type: ignore[override]
        if v is None:
            return []
        if isinstance(v, list):
            # Accept only non-empty strings (mirror original handler behavior)
            return [s.strip() for s in v if isinstance(s, str) and s.strip()]
        return []


class DataValidationEvent(BaseModel):
    source_bucket: str
    source_key: str
    table_name: str
    domain: str
    file_type: str = "csv"
    validation_rules: Optional[Dict[str, Any]] = None
    glue_job_config: Optional[Dict[str, Any]] = None
    step_function_arn: Optional[str] = None


class NotificationEvent(BaseModel):
    notification_type: str = "pipeline_status"
    pipeline_name: str
    domain: str
    table_name: Optional[str] = None
    status: str
    execution_arn: Optional[str] = None
    duration: Optional[str] = None
    environment: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None
    success_details: Optional[Dict[str, Any]] = None


class ErrorContext(BaseModel):
    table_name: Optional[str] = None
    domain: Optional[str] = None
    job_run_id: Optional[str] = None
    step_function_execution_arn: Optional[str] = None


class ErrorEvent(BaseModel):
    source: str
    error_type: Optional[str] = None
    error_message: str
    severity: str = "ERROR"
    context: Optional[ErrorContext] = None


# ===== Transform pipeline models =====


class TransformPreflightEvent(BaseModel):
    """Input model for Transform Preflight Lambda.

    Supports two modes per the state machine contract:
    - Direct mode: provide ds or date_range (XOR)
    - S3 trigger mode: provide source_bucket + source_key

    Also supports legacy alias `table` for `table_name`.
    """

    # Common
    environment: Optional[str] = Field(default=None)
    domain: str
    table_name: Optional[str] = Field(default=None)
    # Legacy alias support
    table: Optional[str] = Field(default=None)
    file_type: str = Field(default="json")

    # Direct mode
    ds: Optional[str] = None
    date_range: Optional[Dict[str, str]] = None  # {start: YYYY-MM-DD, end: YYYY-MM-DD}

    # S3 trigger mode
    source_bucket: Optional[str] = None
    source_key: Optional[str] = None

    @property
    def resolved_table_name(self) -> str:
        value = (self.table_name or self.table or "").strip()
        return value

    @field_validator("file_type")
    @classmethod
    def _normalize_file_type(cls, v: str) -> str:  # type: ignore[override]
        return (v or "json").strip().lower()

    @field_validator("date_range")
    @classmethod
    def _validate_date_range(cls, v: Optional[Dict[str, str]]) -> Optional[Dict[str, str]]:  # type: ignore[override]
        if v is None:
            return None
        # structural check only
        if not isinstance(v, dict) or not v.get("start") or not v.get("end"):
            raise ValueError("date_range must include start and end")
        return v


class TransformPreflightOutput(BaseModel):
    """Output model for Transform Preflight Lambda."""

    proceed: bool
    reason: Optional[str] = None
    ds: Optional[str] = None
    glue_args: Optional[Dict[str, str]] = None
    error: Optional[Dict[str, Any]] = None
