"""Technical indicators utilities for Glue ETL jobs.

This module provides a Pandas-based computation function that can be used
from PySpark via `groupBy('symbol').applyInPandas(...)` to calculate common
technical indicators over OHLCV time series.

Inputs (per symbol):
- date: pandas datetime64[ns] (ascending)
- close, high, low, volume: float/int

Outputs columns (float64); Spark job will round/cast to Decimal as needed:
- sma_20, sma_50, sma_200
- ema_12, ema_26
- rsi_14
- macd, macd_signal, macd_histogram
- bollinger_upper, bollinger_middle, bollinger_lower
- williams_r
- stochastic_k, stochastic_d
- atr_14
- adx_14
- obv
- realized_volatility_20d

Note: EMA/ADX use standard smoothing (Wilder/EMA).
"""

from __future__ import annotations


import numpy as np
import pandas as pd


def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def _rsi_wilder(close: pd.Series, period: int = 14) -> pd.Series:
    """Wilder RSI with edge-case handling.

    - When avg_loss == 0 and avg_gain > 0 -> RSI = 100
    - When avg_gain == 0 and avg_loss > 0 -> RSI = 0
    - When both == 0 -> RSI = 50
    Early rows may still be NaN depending on input length.
    """
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))

    # Edge cases
    rsi = rsi.where(~((avg_loss == 0) & (avg_gain > 0)), 100.0)
    rsi = rsi.where(~((avg_gain == 0) & (avg_loss > 0)), 0.0)
    rsi = rsi.where(~((avg_gain == 0) & (avg_loss == 0)), 50.0)
    return rsi


def _true_range(high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    tr = _true_range(high, low, close)
    # Wilder smoothing approximation using EMA
    return tr.ewm(alpha=1.0 / period, adjust=False).mean()


def _adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    tr = _true_range(high, low, close)
    tr_smooth = pd.Series(tr).ewm(alpha=1.0 / period, adjust=False).mean()
    plus_di = 100.0 * pd.Series(plus_dm).ewm(alpha=1.0 / period, adjust=False).mean() / tr_smooth
    minus_di = 100.0 * pd.Series(minus_dm).ewm(alpha=1.0 / period, adjust=False).mean() / tr_smooth
    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    adx = dx.ewm(alpha=1.0 / period, adjust=False).mean()
    return pd.Series(adx, index=high.index)


def _obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    direction = np.sign(close.diff().fillna(0.0))
    return (direction * volume).fillna(0.0).cumsum()


def _stochastic_kd(close: pd.Series, high: pd.Series, low: pd.Series, period: int = 14) -> tuple[pd.Series, pd.Series]:
    lowest_low = low.rolling(window=period, min_periods=period).min()
    highest_high = high.rolling(window=period, min_periods=period).max()
    denom = (highest_high - lowest_low).replace(0, np.nan)
    k = 100.0 * (close - lowest_low) / denom
    d = k.rolling(window=3, min_periods=3).mean()
    return k, d


def _williams_r(close: pd.Series, high: pd.Series, low: pd.Series, period: int = 14) -> pd.Series:
    lowest_low = low.rolling(window=period, min_periods=period).min()
    highest_high = high.rolling(window=period, min_periods=period).max()
    denom = (highest_high - lowest_low).replace(0, np.nan)
    wr = -100.0 * (highest_high - close) / denom
    return wr


def _bollinger(close: pd.Series, period: int = 20, num_std: float = 2.0) -> tuple[pd.Series, pd.Series, pd.Series]:
    mid = close.rolling(window=period, min_periods=period).mean()
    std = close.rolling(window=period, min_periods=period).std(ddof=0)
    upper = mid + num_std * std
    lower = mid - num_std * std
    return upper, mid, lower


def _realized_volatility(close: pd.Series, period: int = 20) -> pd.Series:
    log_ret = np.log(close / close.shift(1))
    return log_ret.rolling(window=period, min_periods=period).std(ddof=0)


def compute_indicators_pandas(pdf: pd.DataFrame) -> pd.DataFrame:
    """Compute indicators for a single symbol partition.

    Expected columns in `pdf`: [symbol, date, close, high, low, volume, ds]
    Returns original index order with additional indicator columns.
    """
    pdf = pdf.sort_values("date").copy()
    close = pdf["close"].astype(float)
    high = pdf["high"].astype(float)
    low = pdf["low"].astype(float)
    volume = pdf["volume"].astype(float)

    # SMA
    pdf["sma_20"] = close.rolling(window=20, min_periods=20).mean()
    pdf["sma_50"] = close.rolling(window=50, min_periods=50).mean()
    pdf["sma_200"] = close.rolling(window=200, min_periods=200).mean()

    # EMA
    pdf["ema_12"] = _ema(close, 12)
    pdf["ema_26"] = _ema(close, 26)

    # MACD
    macd = pdf["ema_12"] - pdf["ema_26"]
    macd_signal = macd.ewm(span=9, adjust=False).mean()
    pdf["macd"] = macd
    pdf["macd_signal"] = macd_signal
    pdf["macd_histogram"] = macd - macd_signal

    # RSI
    pdf["rsi_14"] = _rsi_wilder(close, 14)

    # Bollinger Bands (20)
    upper, mid, lower = _bollinger(close, 20, 2.0)
    pdf["bollinger_upper"] = upper
    pdf["bollinger_middle"] = mid
    pdf["bollinger_lower"] = lower

    # Williams %R (14)
    pdf["williams_r"] = _williams_r(close, high, low, 14)

    # Stochastic (14)
    k, d = _stochastic_kd(close, high, low, 14)
    pdf["stochastic_k"] = k
    pdf["stochastic_d"] = d

    # ATR(14)
    pdf["atr_14"] = _atr(high, low, close, 14)

    # ADX(14)
    pdf["adx_14"] = _adx(high, low, close, 14)

    # OBV
    pdf["obv"] = _obv(close, volume)

    # Realized volatility (20d, non-annualized)
    pdf["realized_volatility_20d"] = _realized_volatility(close, 20)

    return pdf
