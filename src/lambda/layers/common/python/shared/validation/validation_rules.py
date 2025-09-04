"""Standard validation rules factory (canonical path)."""

from __future__ import annotations

from typing import Any, Dict


class StandardValidationRules:
    @staticmethod
    def get_validation_rules_by_domain(domain: str, table_name: str) -> Dict[str, Any]:
        key = f"{domain.lower()}/{table_name.lower()}"
        presets: Dict[str, Dict[str, Any]] = {
            "customer/transactions": {
                "required_columns": ["transaction_id", "customer_id", "amount", "timestamp"],
                "non_null": ["transaction_id", "customer_id", "amount"],
                "dtypes": {"transaction_id": "str", "customer_id": "str", "amount": "float"},
                "ranges": {"amount": {"min": 0}},
                "allow_duplicates": False,
            },
            "market/prices": {
                "required_columns": ["symbol", "date", "close"],
                "non_null": ["symbol", "date", "close"],
                "dtypes": {"symbol": "str", "date": "date", "close": "float"},
                "allow_duplicates": False,
            },
        }
        default_rules: Dict[str, Any] = {
            "required_columns": [],
            "non_null": [],
            "dtypes": {},
            "ranges": {},
            "allow_duplicates": True,
        }
        return presets.get(key, default_rules)

