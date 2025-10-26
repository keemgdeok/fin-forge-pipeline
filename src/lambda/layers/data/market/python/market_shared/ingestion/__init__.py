"""Market-specific ingestion services."""

from __future__ import annotations

from .service import MarketDataIngestionService, process_event

__all__ = [
    "MarketDataIngestionService",
    "process_event",
]
