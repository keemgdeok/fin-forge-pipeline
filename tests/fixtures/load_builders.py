"""Builders and utilities for Load pipeline tests (Pull model).

These helpers mirror the contracts in docs/specs/load and help keep tests
succinct and intention‑revealing. Production code should live under `src/`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional
from uuid import uuid4


def build_s3_object_created_event(*, bucket: str, key: str, size: int = 2048) -> Dict[str, Any]:
    """Construct a minimal S3 Object Created event payload.

    Mirrors EventBridge event structure needed for input transformation tests.
    """
    return {
        "version": "0",
        "id": str(uuid4()),
        "detail-type": "Object Created",
        "source": "aws.s3",
        "account": "123456789012",
        "time": "2025-09-10T12:00:00Z",
        "region": "ap-northeast-2",
        "resources": [f"arn:aws:s3:::{bucket}"],
        "detail": {
            "bucket": {"name": bucket},
            "object": {"key": key, "size": size},
        },
    }


def build_expected_load_message(
    *,
    bucket: str,
    key: str,
    correlation_id: Optional[str] = None,
    file_size: Optional[int] = None,
    presigned_url: Optional[str] = None,
) -> Dict[str, Any]:
    """Expected SQS message body from the EventBridge transformer, per spec."""
    parts = parse_curated_key(key)
    return {
        "bucket": bucket,
        "key": key,
        "domain": parts.domain,
        "table_name": parts.table_name,
        "interval": parts.interval,
        **({"data_source": parts.data_source} if parts.data_source is not None else {}),
        "year": parts.year,
        "month": parts.month,
        "day": parts.day,
        "layer": parts.layer,
        "ds": parts.ds,
        **({"file_size": file_size} if file_size is not None else {}),
        "correlation_id": correlation_id or str(uuid4()),
        **({"presigned_url": presigned_url} if presigned_url else {}),
    }


@dataclass(frozen=True)
class ParsedCuratedKey:
    domain: str
    table_name: str
    interval: str
    data_source: Optional[str]
    year: str
    month: str
    day: str
    layer: str

    @property
    def ds(self) -> str:
        return f"{self.year}-{self.month}-{self.day}"


def parse_curated_key(key: str) -> ParsedCuratedKey:
    """Parse curated S3 key into its semantic components for tests."""
    parts = key.split("/")
    if len(parts) < 8:
        raise ValueError("Invalid curated key format: not enough segments")

    domain, table_name = parts[0], parts[1]
    interval_segment = parts[2]
    data_source: Optional[str] = None
    index = 3

    if not interval_segment.startswith("interval="):
        raise ValueError("Missing interval segment")
    interval = interval_segment.split("=", 1)[1]

    if parts[3].startswith("data_source="):
        data_source = parts[3].split("=", 1)[1]
        index = 4

    try:
        year_segment = parts[index]
        month_segment = parts[index + 1]
        day_segment = parts[index + 2]
        layer_segment = parts[index + 3]
    except IndexError as exc:
        raise ValueError("Incomplete curated key partitions") from exc

    if not year_segment.startswith("year="):
        raise ValueError("Missing year segment")
    if not month_segment.startswith("month="):
        raise ValueError("Missing month segment")
    if not day_segment.startswith("day="):
        raise ValueError("Missing day segment")
    if not layer_segment.startswith("layer="):
        raise ValueError("Missing layer segment")

    return ParsedCuratedKey(
        domain=domain,
        table_name=table_name,
        interval=interval,
        data_source=data_source,
        year=year_segment.split("=", 1)[1],
        month=month_segment.split("=", 1)[1],
        day=day_segment.split("=", 1)[1],
        layer=layer_segment.split("=", 1)[1],
    )


@dataclass(frozen=True)
class QueueNames:
    main: str
    dlq: str


def build_queue_names(*, env: str, domain: str) -> QueueNames:
    """Build canonical queue names for the load pipeline (spec-compliant)."""
    return QueueNames(main=f"{env}-{domain}-load-queue", dlq=f"{env}-{domain}-load-dlq")
