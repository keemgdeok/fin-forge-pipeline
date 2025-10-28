import market_shared  # noqa: F401  # Ensure provider registration

from shared.validation.validation_rules import StandardValidationRules


def test_standard_rules_known_domain_table() -> None:
    """
    Given: 알려진 도메인/테이블 조합(market/prices)
    When: 표준 검증 규칙 조회
    Then: required_columns 존재, allow_duplicates False
    """
    rules = StandardValidationRules.get_validation_rules_by_domain("market", "prices")
    assert "required_columns" in rules
    assert rules.get("allow_duplicates") is False


def test_standard_rules_default_for_unknown() -> None:
    """
    Given: 알 수 없는 도메인/테이블 조합
    When: 표준 검증 규칙 조회
    Then: required_columns 빈 리스트, allow_duplicates True
    """
    rules = StandardValidationRules.get_validation_rules_by_domain("unknown", "table")
    assert rules["required_columns"] == []
    assert rules["allow_duplicates"] is True
