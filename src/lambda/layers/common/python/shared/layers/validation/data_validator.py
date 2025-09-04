"""Shared data validation utilities used by Lambda functions (layer copy)."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ValidationConfig:
    source_bucket: str
    source_key: str
    file_type: str
    validation_rules: Dict[str, Any]


class DataValidator:
    """Facade for data validation and downstream integrations."""

    def __init__(self, region_name: Optional[str] = None) -> None:
        self.region = region_name or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
        self.s3_client = boto3.client("s3", region_name=self.region)
        self.glue_client = boto3.client("glue", region_name=self.region)
        self.stepfunctions_client = boto3.client("stepfunctions", region_name=self.region)

    def validate_data_comprehensive(self, config: Dict[str, Any]) -> Dict[str, Any]:
        try:
            parsed = self._parse_config(config)
        except ValueError as e:
            logger.error("Validation config error: %s", str(e))
            return {
                "overall_valid": False,
                "errors": [{"type": "ConfigError", "message": str(e)}],
                "checks": [],
            }

        checks: list[Dict[str, Any]] = []
        errors: list[Dict[str, Any]] = []

        try:
            self.s3_client.head_object(Bucket=parsed.source_bucket, Key=parsed.source_key)
            checks.append({"name": "s3_object_exists", "passed": True})
        except ClientError as e:
            msg = f"S3 object not accessible: s3://{parsed.source_bucket}/{parsed.source_key} - {str(e)}"
            logger.warning(msg)
            checks.append({"name": "s3_object_exists", "passed": False})
            errors.append({"type": "S3AccessError", "message": msg})

        allowed_types = {"csv", "json", "parquet"}
        file_type_ok = parsed.file_type.lower() in allowed_types
        checks.append({"name": "file_type_allowed", "passed": file_type_ok, "value": parsed.file_type})
        if not file_type_ok:
            errors.append({"type": "InvalidFileType", "message": f"Unsupported file type: {parsed.file_type}"})

        if parsed.validation_rules:
            checks.append({"name": "rules_present", "passed": True})
        else:
            checks.append({"name": "rules_present", "passed": False})

        overall_valid = all(check.get("passed", False) for check in checks)
        return {"overall_valid": overall_valid, "errors": errors, "checks": checks}

    def trigger_glue_etl_job(self, job_name: str, arguments: Optional[Dict[str, str]] = None) -> Optional[str]:
        try:
            args = arguments or {}
            response = self.glue_client.start_job_run(JobName=job_name, Arguments=args)
            job_run_id = response.get("JobRunId")
            logger.info("Started Glue job %s with run id %s", job_name, job_run_id)
            return job_run_id
        except ClientError as e:
            logger.error("Failed to start Glue job %s: %s", job_name, str(e))
            return None

    @staticmethod
    def _parse_config(config: Dict[str, Any]) -> ValidationConfig:
        missing = [k for k in ("source_bucket", "source_key", "file_type", "validation_rules") if k not in config]
        if missing:
            raise ValueError(f"Missing validation config fields: {missing}")

        return ValidationConfig(
            source_bucket=str(config["source_bucket"]),
            source_key=str(config["source_key"]),
            file_type=str(config["file_type"]),
            validation_rules=dict(config["validation_rules"] or {}),
        )

