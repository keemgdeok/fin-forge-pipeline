"""Reusable IAM helper utilities for security-related constructs."""

from __future__ import annotations

import os
from typing import Iterable, Optional, Sequence, Tuple

from aws_cdk import Stack

from infrastructure.config.types import EnvironmentConfig


def dedupe(values: Iterable[str]) -> list[str]:
    """Return items without duplicates while preserving order."""
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def bucket_arn(bucket_name: str) -> str:
    """Return the ARN for an S3 bucket."""
    bucket = str(bucket_name or "").strip()
    if not bucket:
        raise ValueError("Bucket name must be provided")
    return f"arn:aws:s3:::{bucket}"


def bucket_objects_arn(bucket_name: str, prefix: Optional[str] = None) -> str:
    """Return an object-level ARN for an S3 bucket with an optional prefix."""
    base_arn = bucket_arn(bucket_name)
    if prefix is None or not str(prefix).strip():
        return f"{base_arn}/*"
    normalized = str(prefix).strip().lstrip("/")
    if normalized.endswith("*"):
        return f"{base_arn}/{normalized}"
    return f"{base_arn}/{normalized.rstrip('/')}/*"


def resolve_bootstrap_qualifier(stack: Stack) -> str:
    """Resolve the CDK bootstrap qualifier for the current stack."""
    node = stack.node
    qualifier_raw = (
        node.try_get_context("@aws-cdk/core:bootstrapQualifier")
        or node.try_get_context("bootstrapQualifier")
        or os.getenv("CDK_BOOTSTRAP_QUALIFIER")
        or "hnb659fds"
    )
    qualifier = str(qualifier_raw or "").strip()
    return qualifier or "hnb659fds"


def bootstrap_asset_bucket_name(stack: Stack) -> str:
    """Return the CDK bootstrap asset bucket name for this stack."""
    qualifier = resolve_bootstrap_qualifier(stack)
    account = str(stack.account or "").strip() or "${AWS::AccountId}"
    region = str(stack.region or "").strip() or "${AWS::Region}"
    return f"cdk-{qualifier}-assets-{account}-{region}"


def config_string_list(config: EnvironmentConfig, key: str, default: Sequence[str] = ()) -> list[str]:
    """Return normalized list[str] from config or provide default."""
    raw_values = config.get(key)
    items = list(default if raw_values is None else raw_values)  # type: ignore[arg-type]
    normalized: list[str] = []
    for value in items:
        text = str(value or "").strip()
        if not text:
            continue
        normalized.append(text)
    return dedupe(normalized)


def processing_domain_tables(config: EnvironmentConfig) -> list[Tuple[str, str]]:
    """Return unique (domain, table) pairs configured for processing."""
    pairs: list[Tuple[str, str]] = []
    for trigger in list(config.get("processing_triggers", []) or []):
        domain = str(trigger.get("domain", "")).strip()
        table = str(trigger.get("table_name", "")).strip()
        if domain and table:
            pairs.append((domain, table))

    if not pairs:
        domain = str(config.get("ingestion_domain", "")).strip()
        table = str(config.get("ingestion_table_name", "")).strip()
        if domain and table:
            pairs.append((domain, table))

    result: list[Tuple[str, str]] = []
    seen: set[Tuple[str, str]] = set()
    for pair in pairs:
        if pair in seen:
            continue
        seen.add(pair)
        result.append(pair)
    return result


def curated_subdirectories(config: EnvironmentConfig) -> list[str]:
    """Return curated subdirectories Glue jobs should write into."""
    subdirs: list[str] = []
    curated_layer = str(config.get("curated_layer_name", "") or "").strip()
    if curated_layer:
        subdirs.append(curated_layer)

    subdirs.extend(config_string_list(config, "allowed_load_layers", default=()))

    compaction_subdir = str(config.get("compaction_output_subdir", "") or "").strip()
    if compaction_subdir:
        subdirs.append(compaction_subdir)

    return dedupe(subdirs)
