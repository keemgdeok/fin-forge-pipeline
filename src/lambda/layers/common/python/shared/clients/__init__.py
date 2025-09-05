"""Shared external data source clients.

This package hosts lightweight client facades used by Lambda functions.
Keep implementations dependency-light; optional third-party deps should be
imported lazily and handled gracefully when absent.
"""

from __future__ import annotations

__all__ = [
    "market_data",
]
