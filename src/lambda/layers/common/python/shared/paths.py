"""Utility helpers for building curated S3 prefixes following layer partition scheme."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass(frozen=True)
class CuratedPathComponents:
    """Container describing curated S3 partition components."""

    domain: str
    table: str
    interval: str
    data_source: Optional[str]
    layer: str
    year: str
    month: str
    day: str

    def as_prefix(self) -> str:
        """Return the fully-qualified prefix with layer terminal partition."""
        base = build_curated_interval_prefix(
            domain=self.domain,
            table=self.table,
            interval=self.interval,
            data_source=self.data_source,
        )
        return (
            f"{base}/year={self.year}/month={self.month}/day={self.day}/layer={self.layer}"
        )


def split_ds(ds: str) -> Tuple[str, str, str]:
    """Split an ISO ds (YYYY-MM-DD) into year, month, day components."""

    try:
        year, month, day = ds.split("-")
    except ValueError as exc:  # pragma: no cover - validation handled by callers
        raise ValueError(f"Invalid ds value: {ds}") from exc
    return year, month, day


def build_curated_interval_prefix(
    *,
    domain: str,
    table: str,
    interval: str,
    data_source: Optional[str],
) -> str:
    """Build prefix without date/layer partitions for curated data."""

    base = f"{domain}/{table}/interval={interval}"
    if data_source:
        base = f"{base}/data_source={data_source}"
    return base


def build_curated_layer_prefix(
    *,
    domain: str,
    table: str,
    interval: str,
    data_source: Optional[str],
    layer: str,
) -> str:
    """Prefix up to, but excluding, the year/month/day partitions."""

    base = build_curated_interval_prefix(
        domain=domain,
        table=table,
        interval=interval,
        data_source=data_source,
    )
    return f"{base}/layer={layer}"


def build_curated_layer_path(
    *,
    domain: str,
    table: str,
    interval: str,
    data_source: Optional[str],
    ds: str,
    layer: str,
) -> str:
    """Return fully-qualified curated prefix including layer partition."""

    year, month, day = split_ds(ds)
    base = build_curated_interval_prefix(
        domain=domain,
        table=table,
        interval=interval,
        data_source=data_source,
    )
    return f"{base}/year={year}/month={month}/day={day}/layer={layer}"


def build_curated_components(
    *,
    domain: str,
    table: str,
    interval: str,
    data_source: Optional[str],
    ds: str,
    layer: str,
) -> CuratedPathComponents:
    """Return a dataclass containing partition components for convenience."""

    year, month, day = split_ds(ds)
    return CuratedPathComponents(
        domain=domain,
        table=table,
        interval=interval,
        data_source=data_source,
        layer=layer,
        year=year,
        month=month,
        day=day,
    )
