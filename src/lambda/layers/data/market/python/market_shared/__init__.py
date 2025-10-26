"""Market domain shared utilities exposed via the Lambda layer."""

from __future__ import annotations

from . import validation as _validation  # Ensures provider registration on import

__all__ = [
    "clients",
    "ingestion",
    "validation",
    "activate_market_validation_rules",
]

# Re-export activation helper for explicit bootstrap if needed.
activate_market_validation_rules = _validation.activate_market_validation_rules
