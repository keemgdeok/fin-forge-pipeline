"""Validation rule provider for the market domain."""

from __future__ import annotations

from .rules import MarketValidationRuleProvider, activate_market_validation_rules

__all__ = [
    "MarketValidationRuleProvider",
    "activate_market_validation_rules",
]
