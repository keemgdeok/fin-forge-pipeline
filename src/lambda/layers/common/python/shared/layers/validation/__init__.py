"""Legacy import path shim for validation utilities.

Prefer importing from 'shared.validation', e.g.:
    from shared.validation import DataValidator, StandardValidationRules
"""

from shared.validation import DataValidator, StandardValidationRules  # noqa: F401
