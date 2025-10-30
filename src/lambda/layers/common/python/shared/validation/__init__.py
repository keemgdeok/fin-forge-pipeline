"""Validation utilities exposed by the shared layer (simplified path).

Canonical import path:
    from shared.validation import DataValidator, StandardValidationRules
"""

from .data_validator import DataValidator  # noqa: F401
from .validation_rules import (  # noqa: F401
    StandardValidationRules,
    ValidationRuleProvider,
    get_validation_rule_provider,
    register_validation_rule_provider,
)

__all__ = [
    "DataValidator",
    "StandardValidationRules",
    "ValidationRuleProvider",
    "get_validation_rule_provider",
    "register_validation_rule_provider",
]
