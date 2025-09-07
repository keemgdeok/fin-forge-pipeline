from __future__ import annotations

"""Reusable, typed builders for test data.

Keep builders small and explicit. Prefer simple dicts with clear defaults
so that tests remain readable and intention-revealing.
"""

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
    symbol: str,
    period: str,
    interval: str,
    date: Optional[datetime] = None,
) -> str:
    """Compose RAW bucket prefix for a given symbol/day consistent with service logic.

    Mirrors the partitioning used in shared.ingestion.service._compose_s3_key.
    """
    d = (date or datetime.now(timezone.utc)).strftime("%Y-%m-%d")
    return (
        f"{domain}/{table_name}/ingestion_date={d}/"
        f"data_source={data_source}/symbol={symbol}/interval={interval}/period={period}/"
    )
