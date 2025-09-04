from shared.validation.validation_rules import StandardValidationRules


def test_standard_rules_known_domain_table():
    rules = StandardValidationRules.get_validation_rules_by_domain("market", "prices")
    assert "required_columns" in rules
    assert rules.get("allow_duplicates") is False


def test_standard_rules_default_for_unknown():
    rules = StandardValidationRules.get_validation_rules_by_domain("unknown", "table")
    assert rules["required_columns"] == []
    assert rules["allow_duplicates"] is True

