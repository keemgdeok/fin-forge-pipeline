"""Contracts for ingestion services shared across domains."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Protocol


@dataclass(frozen=True)
class IngestionResult:
    """Result of executing an ingestion workflow."""

    written_keys: List[str] = field(default_factory=list)
    manifest_objects: Dict[date, Dict[str, Any]] = field(default_factory=dict)


class IngestionService(Protocol):
    """Protocol representing ingestion workflow implementations."""

    def process_event(self, event: Dict[str, Any], context: Any) -> Dict[str, Any]:
        ...
