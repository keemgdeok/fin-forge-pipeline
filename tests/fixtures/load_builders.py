"""Builders and utilities for Load pipeline tests (Pull model).

These helpers mirror the contracts in docs/specs/load and help keep tests
succinct and intention‑revealing. Production code should live under `src/`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
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
    domain, table, partition = parse_curated_key(key)
    return {
        "bucket": bucket,
        "key": key,
        "domain": domain,
        "table_name": table,
        "partition": partition,
        **({"file_size": file_size} if file_size is not None else {}),
        "correlation_id": correlation_id or str(uuid4()),
        **({"presigned_url": presigned_url} if presigned_url else {}),
    }


def parse_curated_key(key: str) -> Tuple[str, str, str]:
    """Parse curated S3 key into (domain, table_name, partition).

    Expected format: <domain>/<table_name>/ds=YYYY-MM-DD/<file>.parquet
    This is a test helper; production code should implement the parser and be
    covered by tests in tests/unit/load/contracts/test_key_parsing.py.
    """
    parts = key.split("/")
    if len(parts) < 4:
        raise ValueError("Invalid curated key format: not enough segments")
    domain, table_name, partition = parts[0], parts[1], parts[2]
    if not partition.startswith("ds="):
        raise ValueError("Invalid partition segment; expected ds=YYYY-MM-DD")
    return domain, table_name, partition


@dataclass(frozen=True)
class QueueNames:
    main: str
    dlq: str


def build_queue_names(*, env: str, domain: str) -> QueueNames:
    """Build canonical queue names for the load pipeline (spec-compliant)."""
    return QueueNames(main=f"{env}-{domain}-load-queue", dlq=f"{env}-{domain}-load-dlq")
