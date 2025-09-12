from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass
class DQConfig:
    expected_min_records: int
    max_critical_error_rate: float  # percentage, e.g., 5.0 for 5%


@dataclass
class DQMetrics:
    record_count: int
    null_symbol_count: int = 0
    negative_price_count: int = 0
    duplicate_key_groups: int = 0
    invalid_numeric_type_issues: int = 0


@dataclass
class DQResult:
    ok: bool
    action: str  # "proceed" | "quarantine"
    critical_violations: int
    warning_violations: int
    critical_error_rate: float
    messages: List[str]


def _pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return (numerator / denominator) * 100.0


def evaluate(metrics: DQMetrics, config: DQConfig) -> DQResult:
    """Evaluate data quality based on collected metrics and configuration.

    This pure-Python evaluator does not depend on Spark. The Glue job should
    collect counts and schema issues, then pass them here for a decision.
    """
    msgs: List[str] = []
    warnings = 0

    # Guard: empty dataset scenario (treated as critical failure upstream)
    if metrics.record_count <= 0:
        msgs.append("NO_RAW_DATA: No records found for partition")
        return DQResult(
            ok=False,
            action="quarantine",
            critical_violations=0,
            warning_violations=0,
            critical_error_rate=0.0,
            messages=msgs,
        )

    # Critical violations (per-record issues)
    critical = 0

    if metrics.null_symbol_count > 0:
        critical += metrics.null_symbol_count
        msgs.append(f"null symbol present ({metrics.null_symbol_count} records)")

    if metrics.negative_price_count > 0:
        critical += metrics.negative_price_count
        msgs.append(f"negative price present ({metrics.negative_price_count} records)")

    # Warnings (group-based, schema-level, and completeness checks)
    if metrics.duplicate_key_groups > 0:
        warnings += metrics.duplicate_key_groups
        msgs.append(f"duplicate key detected ({metrics.duplicate_key_groups} groups)")

    if metrics.record_count < config.expected_min_records:
        warnings += 1
        msgs.append(f"low record count: {metrics.record_count} < {config.expected_min_records}")

    if metrics.invalid_numeric_type_issues > 0:
        warnings += metrics.invalid_numeric_type_issues
        msgs.append(f"invalid numeric type issues: {metrics.invalid_numeric_type_issues}")

    cer = _pct(critical, metrics.record_count)

    if critical > 0:
        msgs.append(f"Critical Error Rate: {cer:.2f}% (threshold: {config.max_critical_error_rate}%)")

    if cer > config.max_critical_error_rate:
        return DQResult(
            ok=False,
            action="quarantine",
            critical_violations=critical,
            warning_violations=warnings,
            critical_error_rate=cer,
            messages=msgs,
        )

    return DQResult(
        ok=True,
        action="proceed",
        critical_violations=critical,
        warning_violations=warnings,
        critical_error_rate=cer,
        messages=msgs if msgs else ["DQ OK"],
    )
