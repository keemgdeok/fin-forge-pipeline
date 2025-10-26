"""Market domain validation rule provider."""

from __future__ import annotations

from typing import Any, Dict

from shared.validation.validation_rules import (
    ValidationRuleProvider,
    get_validation_rule_provider,
    register_validation_rule_provider,
)


_DEFAULT_RULES: Dict[str, Any] = {
    "required_columns": [],
    "non_null": [],
    "dtypes": {},
    "ranges": {},
    "allow_duplicates": True,
}


_MARKET_RULES: Dict[str, Dict[str, Any]] = {
    "market/prices": {
        "required_columns": ["symbol", "date", "close"],
        "non_null": ["symbol", "date", "close"],
        "dtypes": {"symbol": "str", "date": "date", "close": "float"},
        "allow_duplicates": False,
    },
}


class MarketValidationRuleProvider(ValidationRuleProvider):
    """Provides validation presets for market-oriented datasets."""

    def get_validation_rules(self, *, domain: str, table_name: str) -> Dict[str, Any]:
        key = f"{domain.lower()}/{table_name.lower()}"
        return dict(_MARKET_RULES.get(key, _DEFAULT_RULES))


def activate_market_validation_rules() -> None:
    """Ensure the market provider is registered as the active rule provider."""

    current = get_validation_rule_provider()
    if not isinstance(current, MarketValidationRuleProvider):
        register_validation_rule_provider(MarketValidationRuleProvider())


# Register provider on import so that Lambda cold starts pick up rules automatically.
activate_market_validation_rules()
