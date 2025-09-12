from shared.dq.engine import DQConfig, DQMetrics, evaluate
import pytest


def test_empty_dataset_quarantine() -> None:
    cfg = DQConfig(expected_min_records=10, max_critical_error_rate=5.0)
    metrics = DQMetrics(record_count=0)
    result = evaluate(metrics, cfg)
    assert result.ok is False
    assert result.action == "quarantine"
    assert any("NO_RAW_DATA" in m for m in result.messages)


def test_critical_rate_exceeds_threshold_quarantine() -> None:
    cfg = DQConfig(expected_min_records=50, max_critical_error_rate=5.0)
    metrics = DQMetrics(
        record_count=100,
        null_symbol_count=6,  # 6%
        negative_price_count=0,
        duplicate_key_groups=2,
        invalid_numeric_type_issues=1,
    )
    result = evaluate(metrics, cfg)
    assert result.ok is False
    assert result.action == "quarantine"
    assert result.critical_violations == 6
    assert result.warning_violations >= 1
    assert result.critical_error_rate > cfg.max_critical_error_rate


def test_within_threshold_proceed_with_warnings() -> None:
    cfg = DQConfig(expected_min_records=50, max_critical_error_rate=5.0)
    metrics = DQMetrics(
        record_count=100,
        null_symbol_count=2,  # 2%
        negative_price_count=1,  # +1% => 3%
        duplicate_key_groups=1,
        invalid_numeric_type_issues=1,  # add one schema-level warning
    )
    result = evaluate(metrics, cfg)
    assert result.ok is True
    assert result.action == "proceed"
    assert result.critical_violations == 3
    assert 2 <= result.warning_violations  # at least duplicate + maybe others
    assert 2.0 < result.critical_error_rate < 5.0
    assert any("null symbol" in m for m in result.messages)


def test_low_record_count_is_warning_not_critical() -> None:
    cfg = DQConfig(expected_min_records=10, max_critical_error_rate=5.0)
    metrics = DQMetrics(record_count=9)
    result = evaluate(metrics, cfg)
    assert result.ok is True  # no critical violations
    assert result.action == "proceed"
    assert result.critical_violations == 0
    assert result.warning_violations >= 1
    assert any("low record count" in m for m in result.messages)


@pytest.mark.parametrize(
    "record_count,critical,total_expected_rate,expect_quarantine",
    [
        (100, 5, 5.0, False),  # exactly at threshold -> proceed
        (100, 4, 4.0, False),  # below threshold -> proceed
        (100, 6, 6.0, True),  # above threshold -> quarantine
    ],
)
def test_threshold_boundaries(
    record_count: int, critical: int, total_expected_rate: float, expect_quarantine: bool
) -> None:
    cfg = DQConfig(expected_min_records=1, max_critical_error_rate=5.0)
    # Split critical into two sources to ensure summation behavior
    ns = critical // 2
    np = critical - ns
    metrics = DQMetrics(
        record_count=record_count,
        null_symbol_count=ns,
        negative_price_count=np,
    )
    result = evaluate(metrics, cfg)
    assert (result.action == "quarantine") is expect_quarantine
    assert pytest.approx(result.critical_error_rate, rel=1e-3) == total_expected_rate


def test_no_violations_returns_ok_message() -> None:
    cfg = DQConfig(expected_min_records=1, max_critical_error_rate=5.0)
    metrics = DQMetrics(record_count=10)
    result = evaluate(metrics, cfg)
    assert result.ok is True
    assert result.action == "proceed"
    assert result.critical_violations == 0
    assert result.warning_violations == 0
    assert result.messages == ["DQ OK"]


def test_warning_sources_increment_only_warnings() -> None:
    cfg = DQConfig(expected_min_records=50, max_critical_error_rate=5.0)
    metrics = DQMetrics(
        record_count=100,
        duplicate_key_groups=3,
        invalid_numeric_type_issues=2,
    )
    result = evaluate(metrics, cfg)
    assert result.ok is True
    assert result.critical_violations == 0
    assert result.warning_violations >= 5
