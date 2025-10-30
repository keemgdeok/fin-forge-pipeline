"""Reusable, typed builders for test data.

Keep builders small and explicit. Prefer simple dicts with clear defaults
so that tests remain readable and intention-revealing.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


# Fixed timestamp for deterministic records in tests
DEFAULT_TS: datetime = datetime(2024, 1, 1, tzinfo=timezone.utc)


def build_ingestion_event(
    *,
    symbols: Optional[List[Any]] = None,
    data_source: str = "yahoo_finance",
    data_type: str = "prices",
    domain: str = "market",
    table_name: str = "prices",
    period: str = "1mo",
    interval: str = "1d",
    file_format: str = "json",
    overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a minimal ingestion/orchestrator event payload.

    The shape matches what our orchestrator and worker handlers expect.
    """
    event: Dict[str, Any] = {
        "data_source": data_source,
        "data_type": data_type,
        "symbols": symbols or [],
        "period": period,
        "interval": interval,
        "domain": domain,
        "table_name": table_name,
        "file_format": file_format,
    }
    if overrides:
        event.update(overrides)
    return event


def build_raw_s3_prefix(
    *,
    domain: str,
    table_name: str,
    data_source: str,
    interval: str,
    date: Optional[datetime] = None,
) -> str:
    """Compose RAW bucket day-level prefix following the interval/source partition scheme."""

    dt = (date or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return (
        f"{domain}/{table_name}/"
        f"interval={interval}/"
        f"data_source={data_source}/"
        f"year={dt.year:04d}/month={dt.month:02d}/day={dt.day:02d}/"
    )


def build_raw_s3_object_key(
    *,
    domain: str,
    table_name: str,
    data_source: str,
    interval: str,
    symbol: str,
    extension: str = "json",
    date: Optional[datetime] = None,
) -> str:
    """Compose full RAW bucket object key for a given symbol/day."""

    prefix = build_raw_s3_prefix(
        domain=domain,
        table_name=table_name,
        data_source=data_source,
        interval=interval,
        date=date,
    )
    return f"{prefix}{symbol}.{extension}"


def build_raw_manifest_key(
    *,
    domain: str,
    table_name: str,
    data_source: str,
    interval: str,
    basename: str = "_batch",
    suffix: str = ".manifest.json",
    date: Optional[datetime] = None,
) -> str:
    """Compose RAW bucket manifest object key for a given day."""

    if suffix and not suffix.startswith("."):
        suffix = f".{suffix}"

    prefix = build_raw_s3_prefix(
        domain=domain,
        table_name=table_name,
        data_source=data_source,
        interval=interval,
        date=date,
    )
    return f"{prefix}{basename}{suffix}"


def build_transform_event(
    *,
    domain: str = "market",
    table_name: str = "prices",
    ds: Optional[str] = None,
    date_range: Optional[Dict[str, str]] = None,
    source_bucket: Optional[str] = None,
    source_key: Optional[str] = None,
    file_type: str = "json",
    environment: str = "dev",
    reprocess: bool = False,
    execution_id: Optional[str] = None,
    catalog_update: str = "on_schema_change",
    overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a transform pipeline input event payload.

    Follows the transform state machine contract specification.
    """
    event: Dict[str, Any] = {
        "environment": environment,
        "domain": domain,
        "table_name": table_name,
        "file_type": file_type,
        "reprocess": reprocess,
        "catalog_update": catalog_update,
    }

    # Add optional fields
    if ds:
        event["ds"] = ds
    if date_range:
        event["date_range"] = date_range
    if source_bucket:
        event["source_bucket"] = source_bucket
    if source_key:
        event["source_key"] = source_key
    if execution_id:
        event["execution_id"] = execution_id

    if overrides:
        event.update(overrides)

    return event


def build_raw_market_data(
    records: Optional[List[Dict[str, Any]]] = None,
    *,
    default_symbol: str = "AAPL",
    default_price: float = 150.00,
    default_exchange: str = "NASDAQ",
    base_timestamp: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Build realistic market data records for testing.

    Creates structured data that matches expected market data schema.
    """
    if records:
        return records

    base_ts = base_timestamp or datetime.now(timezone.utc)

    return [
        {
            "symbol": default_symbol,
            "price": default_price,
            "exchange": default_exchange,
            "timestamp": base_ts.isoformat() + "Z",
            "volume": 1000000,
            "bid": default_price - 0.05,
            "ask": default_price + 0.05,
        }
    ]


def build_curated_s3_key(
    *,
    domain: str,
    table_name: str,
    ds: str,
    file_name: str = "part-0000.parquet",
) -> str:
    """Build curated S3 key following partitioning scheme."""
    return f"{domain}/{table_name}/ds={ds}/{file_name}"


def build_schema_fingerprint(
    *,
    columns: Optional[List[Dict[str, str]]] = None,
    codec: str = "zstd",
    additional_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build schema fingerprint for testing schema evolution."""
    if columns is None:
        columns = [
            {"name": "symbol", "type": "string"},
            {"name": "price", "type": "double"},
            {"name": "exchange", "type": "string"},
            {"name": "timestamp", "type": "timestamp"},
        ]

    fingerprint = {
        "columns": columns,
        "codec": codec,
        "hash": "test_hash_" + str(hash(str(columns))),
        "created_at": datetime.now(timezone.utc).isoformat() + "Z",
    }

    if additional_metadata:
        fingerprint.update(additional_metadata)

    return fingerprint


def build_glue_job_args(
    *,
    ds: str,
    raw_bucket: str = "test-raw-bucket",
    raw_prefix: str = "market/prices/interval=1d/data_source=yahoo_finance/",
    curated_bucket: str = "test-curated-bucket",
    curated_prefix: str = "market/prices/",
    artifacts_bucket: str = "test-artifacts-bucket",
    codec: str = "zstd",
    target_file_mb: int = 256,
    file_type: str = "json",
    schema_fingerprint_s3_uri: Optional[str] = None,
    overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build Glue job arguments following specification."""
    if schema_fingerprint_s3_uri is None:
        domain = raw_prefix.split("/")[0]
        table = raw_prefix.split("/")[1]
        schema_fingerprint_s3_uri = f"s3://{artifacts_bucket}/{domain}/{table}/_schema/latest.json"

    args = {
        "--ds": ds,
        "--raw_bucket": raw_bucket,
        "--raw_prefix": raw_prefix,
        "--curated_bucket": curated_bucket,
        "--curated_prefix": curated_prefix,
        "--job-bookmark-option": "job-bookmark-enable",
        "--enable-s3-parquet-optimized-committer": "1",
        "--codec": codec,
        "--target_file_mb": str(target_file_mb),
        "--schema_fingerprint_s3_uri": schema_fingerprint_s3_uri,
        "--file_type": file_type,
    }

    if overrides:
        args.update(overrides)

    return args


def build_data_quality_violation(
    *,
    rule_name: str,
    severity: str = "critical",
    violating_records: Optional[List[Dict[str, Any]]] = None,
    message: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build data quality violation for testing."""
    violation = {
        "rule": rule_name,
        "severity": severity,
        "count": len(violating_records) if violating_records else 0,
        "message": message or f"{rule_name} violation detected",
        "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
    }

    if violating_records:
        violation["violating_records"] = violating_records

    if metadata:
        violation["metadata"] = metadata

    return violation
