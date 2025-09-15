"""Unit tests for indicators calculation library.

Covers a subset of indicators with deterministic inputs to validate formulas
and windowing behavior. Uses Pandas directly (no Spark required).
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure project root and 'src' are importable in environments where conftest path
# hooks may not run before module import (e.g., some CI collectors).
_root = Path(__file__).resolve().parents[4]
for _p in (str(_root), str(_root / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

try:  # noqa: E402
    # Preferred when 'src' is on sys.path (namespace package under src)
    from glue.lib.indicators import compute_indicators_pandas  # type: ignore
except ModuleNotFoundError:  # noqa: E402
    # Fallback: import via explicit src-qualified package
    from src.glue.lib.indicators import compute_indicators_pandas  # type: ignore


def _make_series(n: int = 40) -> pd.DataFrame:
    dates = pd.date_range("2025-01-01", periods=n, freq="D")
    # Monotonic increasing close; simple high/low bands and constant volume
    close = np.linspace(100, 139, n)
    high = close + 1.0
    low = close - 1.0
    volume = np.full(n, 100.0)
    df = pd.DataFrame(
        {
            "symbol": ["AAA"] * n,
            "date": dates,
            "ds": dates.strftime("%Y-%m-%d"),
            "open": close,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        }
    )
    return df


def test_sma_bollinger_obv_basic() -> None:
    pdf = _make_series(40)
    out = compute_indicators_pandas(pdf)

    # SMA20 at the last row equals rolling mean of last 20 closes
    exp_sma20 = pd.Series(pdf["close"]).rolling(window=20, min_periods=20).mean().iloc[-1]
    assert np.isclose(out["sma_20"].iloc[-1], exp_sma20, rtol=1e-6, atol=1e-6)

    # Bollinger middle equals SMA20; band width equals 2*std
    exp_mid = exp_sma20
    exp_std = pd.Series(pdf["close"]).rolling(window=20, min_periods=20).std(ddof=0).iloc[-1]
    exp_upper = exp_mid + 2.0 * exp_std
    exp_lower = exp_mid - 2.0 * exp_std
    assert np.isclose(out["bollinger_middle"].iloc[-1], exp_mid, atol=1e-6)
    assert np.isclose(out["bollinger_upper"].iloc[-1], exp_upper, atol=1e-6)
    assert np.isclose(out["bollinger_lower"].iloc[-1], exp_lower, atol=1e-6)

    # OBV increases by volume each day since close is strictly increasing
    exp_obv_last = 0.0
    for i in range(1, len(pdf)):
        exp_obv_last += 100.0  # volume
    assert np.isclose(out["obv"].iloc[-1], exp_obv_last, atol=1e-6)


def test_macd_signal_histogram_consistency() -> None:
    pdf = _make_series(40)
    out = compute_indicators_pandas(pdf)
    macd = out["macd"].iloc[-1]
    sig = out["macd_signal"].iloc[-1]
    hist = out["macd_histogram"].iloc[-1]
    assert np.isclose(hist, macd - sig, atol=1e-6)


def test_stochastic_rsi_ranges_and_nans() -> None:
    pdf = _make_series(40)
    out = compute_indicators_pandas(pdf)

    # RSI range 0..100, Stochastic K/D 0..100 (after enough periods)
    rsi_tail = out["rsi_14"].dropna().iloc[-1]
    k_tail = out["stochastic_k"].dropna().iloc[-1]
    d_tail = out["stochastic_d"].dropna().iloc[-1]
    assert 0.0 <= rsi_tail <= 100.0
    assert 0.0 <= k_tail <= 100.0
    assert 0.0 <= d_tail <= 100.0

    # Early rows before window length should be NaN
    assert pd.isna(out["sma_20"].iloc[0])
    assert pd.isna(out["rsi_14"].iloc[0])
    assert pd.isna(out["stochastic_k"].iloc[0])
