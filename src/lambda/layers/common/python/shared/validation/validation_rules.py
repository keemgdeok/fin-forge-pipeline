"""Validation rule provider registry."""

from __future__ import annotations

from typing import Any, Dict, Protocol


class ValidationRuleProvider(Protocol):
    """Protocol representing configurable validation rule providers."""

    def get_validation_rules(self, *, domain: str, table_name: str) -> Dict[str, Any]:
        ...


class _DefaultValidationRuleProvider:
    def get_validation_rules(self, *, domain: str, table_name: str) -> Dict[str, Any]:  # noqa: D401
        return {
            "required_columns": [],
            "non_null": [],
            "dtypes": {},
            "ranges": {},
            "allow_duplicates": True,
        }


_provider: ValidationRuleProvider = _DefaultValidationRuleProvider()


def register_validation_rule_provider(provider: ValidationRuleProvider) -> None:
    """Register the active validation rule provider."""

    global _provider
    _provider = provider


def get_validation_rule_provider() -> ValidationRuleProvider:
    """Return the currently registered validation rule provider."""

    return _provider


class StandardValidationRules:
    @staticmethod
    def get_validation_rules_by_domain(domain: str, table_name: str) -> Dict[str, Any]:
        provider = get_validation_rule_provider()
        return provider.get_validation_rules(domain=domain, table_name=table_name)
