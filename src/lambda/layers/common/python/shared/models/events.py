"""Typed event models for Lambda handlers using Pydantic v2."""

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

