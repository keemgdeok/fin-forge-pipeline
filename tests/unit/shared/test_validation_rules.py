import market_shared  # noqa: F401  # Ensure provider registration

from shared.validation.validation_rules import StandardValidationRules


def test_standard_rules_known_domain_table() -> None:
    """
    Given: 알려진 도메인/테이블 조합(market/prices)
    When: 표준 검증 규칙을 조회하면
    Then: 필수 컬럼 목록이 존재하고 allow_duplicates가 False여야 함
    """
    rules = StandardValidationRules.get_validation_rules_by_domain("market", "prices")
    assert "required_columns" in rules
    assert rules.get("allow_duplicates") is False


def test_standard_rules_default_for_unknown() -> None:
    """
    Given: 알 수 없는 도메인/테이블 조합
    When: 표준 검증 규칙을 조회하면
    Then: 기본 규칙(필수 컬럼 없음, allow_duplicates=True)이 반환되어야 함
    """
    rules = StandardValidationRules.get_validation_rules_by_domain("unknown", "table")
    assert rules["required_columns"] == []
    assert rules["allow_duplicates"] is True
