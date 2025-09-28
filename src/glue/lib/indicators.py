"""Comprehensive technical indicators toolkit for Glue ETL jobs.

The functions in this module operate on Pandas DataFrames and are typically
invoked from PySpark via ``groupBy('symbol').applyInPandas(...)``.

Expected input columns per symbol partition:
- symbol: string identifier
- date: pandas ``datetime64[ns]`` (ascending order)
- ds: partition string (``YYYY-MM-DD``)
- open, high, low, close, volume: numeric columns
- Optional benchmark columns: ``market_close``, ``benchmark_close``,
  ``market_return`` (decimal) or ``market_return_1d`` (percentage).
- Optional ``vwap`` column when intraday VWAP has already been prepared upstream.

Outputs align with the curated indicators table specification and include the
full set of risk/volatility, momentum, and quality control fields.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class Bounds:
    """Value bounds that should be satisfied by indicator outputs."""

    lower: Optional[float] = None
    upper: Optional[float] = None


def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def _wilder(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(alpha=1.0 / period, adjust=False).mean()


def _true_range(high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
    prev_close = close.shift(1)
    ranges = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    )
    return ranges.max(axis=1)


def _rsi_wilder(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = _wilder(gain, period)
    avg_loss = _wilder(loss, period)

    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))

    rsi = rsi.where(~((avg_loss == 0.0) & (avg_gain > 0.0)), 100.0)
    rsi = rsi.where(~((avg_gain == 0.0) & (avg_loss > 0.0)), 0.0)
    rsi = rsi.where(~((avg_gain == 0.0) & (avg_loss == 0.0)), 50.0)
    return rsi


def _stochastic_kd(close: pd.Series, high: pd.Series, low: pd.Series, period: int = 14) -> Tuple[pd.Series, pd.Series]:
    lowest_low = low.rolling(window=period, min_periods=period).min()
    highest_high = high.rolling(window=period, min_periods=period).max()
    denom = (highest_high - lowest_low).replace(0.0, np.nan)
    k = 100.0 * (close - lowest_low) / denom
    d = k.rolling(window=3, min_periods=3).mean()
    return k, d


def _williams_r(close: pd.Series, high: pd.Series, low: pd.Series, period: int = 14) -> pd.Series:
    lowest_low = low.rolling(window=period, min_periods=period).min()
    highest_high = high.rolling(window=period, min_periods=period).max()
    denom = (highest_high - lowest_low).replace(0.0, np.nan)
    return -100.0 * (highest_high - close) / denom


def _bollinger(close: pd.Series, period: int = 20, num_std: float = 2.0) -> Tuple[pd.Series, pd.Series, pd.Series]:
    mid = close.rolling(window=period, min_periods=period).mean()
    std = close.rolling(window=period, min_periods=period).std(ddof=0)
    upper = mid + num_std * std
    lower = mid - num_std * std
    return upper, mid, lower


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    return _wilder(_true_range(high, low, close), period)


def _directional_indicators(
    high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0.0), up_move, 0.0), index=high.index)
    minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0.0), down_move, 0.0), index=high.index)

    tr = _true_range(high, low, close)
    tr_smooth = _wilder(tr, period)
    plus_di = 100.0 * _wilder(plus_dm, period) / tr_smooth.replace(0.0, np.nan)
    minus_di = 100.0 * _wilder(minus_dm, period) / tr_smooth.replace(0.0, np.nan)

    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, np.nan)
    adx = _wilder(dx, period)
    return adx, plus_di, minus_di


def _obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    direction = np.sign(close.diff().fillna(0.0))
    return (direction * volume.fillna(0.0)).cumsum()


def _money_flow_index(
    high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series, period: int = 14
) -> pd.Series:
    typical_price = (high + low + close) / 3.0
    money_flow = typical_price * volume
    price_delta = typical_price.diff()

    positive_flow = money_flow.where(price_delta > 0.0, 0.0)
    negative_flow = money_flow.where(price_delta < 0.0, 0.0).abs()

    pos_roll = positive_flow.rolling(window=period, min_periods=period).sum()
    neg_roll = negative_flow.rolling(window=period, min_periods=period).sum()

    money_ratio = pos_roll / neg_roll.replace(0.0, np.nan)
    mfi = 100.0 - (100.0 / (1.0 + money_ratio))
    mfi = mfi.where(~((neg_roll == 0.0) & (pos_roll > 0.0)), 100.0)
    mfi = mfi.where(~((pos_roll == 0.0) & (neg_roll > 0.0)), 0.0)
    mfi = mfi.where(~((pos_roll == 0.0) & (neg_roll == 0.0)), np.nan)
    return mfi


def _commodity_channel_index(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 20) -> pd.Series:
    typical_price = (high + low + close) / 3.0
    sma_tp = typical_price.rolling(window=period, min_periods=period).mean()

    def _mean_abs_dev(values: np.ndarray) -> float:
        center = values.mean()
        return np.mean(np.abs(values - center))

    mad = typical_price.rolling(window=period, min_periods=period).apply(_mean_abs_dev, raw=True)
    denom = (0.015 * mad).replace(0.0, np.nan)
    return (typical_price - sma_tp) / denom


def _chaikin_money_flow(
    high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series, period: int = 20
) -> Tuple[pd.Series, pd.Series]:
    high_low_range = (high - low).replace(0.0, np.nan)
    multiplier = ((close - low) - (high - close)) / high_low_range
    multiplier = multiplier.fillna(0.0)
    money_flow_volume = multiplier * volume.fillna(0.0)

    rolling_mfv = money_flow_volume.rolling(window=period, min_periods=period).sum()
    rolling_volume = volume.rolling(window=period, min_periods=period).sum()
    cmf = rolling_mfv / rolling_volume.replace(0.0, np.nan)

    adi = money_flow_volume.cumsum()
    return cmf, adi


def _rate_of_change(close: pd.Series, period: int) -> pd.Series:
    return (close / close.shift(period) - 1.0) * 100.0


def _realized_volatility(log_returns: pd.Series, period: int, annualize: bool = False) -> pd.Series:
    std = log_returns.rolling(window=period, min_periods=period).std(ddof=1)
    scale = np.sqrt(252.0) if annualize else 1.0
    return std * scale * 100.0


def _parkinson_volatility(high: pd.Series, low: pd.Series, period: int = 20) -> pd.Series:
    hl_log = np.log((high / low).replace(0.0, np.nan))
    hl_sq = hl_log.pow(2)
    mean_hl_sq = hl_sq.rolling(window=period, min_periods=period).mean()
    return np.sqrt(mean_hl_sq / (4.0 * np.log(2.0))) * np.sqrt(252.0) * 100.0


def _garman_klass_volatility(
    open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, period: int = 20
) -> pd.Series:
    safe_open = open_.replace(0.0, np.nan)
    log_hl = np.log((high / low).replace(0.0, np.nan))
    log_co = np.log((close / safe_open).replace(0.0, np.nan))

    rs = 0.5 * log_hl.pow(2) - (2.0 * np.log(2.0) - 1.0) * log_co.pow(2)
    rs_mean = rs.rolling(window=period, min_periods=period).mean()
    return np.sqrt(rs_mean * 252.0) * 100.0


def _prepare_market_returns(pdf: pd.DataFrame) -> Optional[pd.Series]:
    for candidate in ("market_return", "market_return_1d", "benchmark_return"):
        if candidate in pdf.columns:
            series = pd.to_numeric(pdf[candidate], errors="coerce")
            if series.abs().max(skipna=True) > 10.0:
                return series / 100.0
            return series

    for candidate in ("market_close", "benchmark_close", "sp500_close", "spy_close"):
        if candidate in pdf.columns:
            series = pd.to_numeric(pdf[candidate], errors="coerce")
            return series.pct_change()
    return None


def _apply_bounds(series: pd.Series, bounds: Bounds) -> Tuple[pd.Series, pd.Series]:
    if series.empty:
        return series, series.astype(bool)

    mask = pd.Series(True, index=series.index)
    if bounds.lower is not None:
        mask &= series >= bounds.lower
    if bounds.upper is not None:
        mask &= series <= bounds.upper

    valid_mask = mask | series.isna()
    adjusted = series.where(valid_mask, np.nan)
    violations = (~valid_mask) & series.notna()
    return adjusted, violations


def _apply_non_negative(series: pd.Series) -> Tuple[pd.Series, pd.Series]:
    return _apply_bounds(series, Bounds(lower=0.0))


def compute_indicators_pandas(pdf: pd.DataFrame) -> pd.DataFrame:
    """Compute indicators for a single symbol partition.

    Parameters
    ----------
    pdf:
        Pandas DataFrame containing a *single* symbol worth of data.

    Returns
    -------
    pd.DataFrame
        DataFrame with the full curated indicators schema.
    """

    output_columns = [
        "date",
        "symbol",
        "ds",
        "sma_20",
        "sma_60",
        "sma_120",
        "ema_20",
        "ema_60",
        "return_1d",
        "return_5d",
        "return_20d",
        "log_return_1d",
        "envelope_mid_20_3",
        "envelope_upper_20_3",
        "envelope_lower_20_3",
        "rsi_14",
        "rsi_ema6",
        "macd_12_26",
        "macd_signal_9",
        "macd_hist_12_26_9",
        "macd_pct_12_26",
        "bollinger_middle_20_2",
        "bollinger_upper_20_2",
        "bollinger_lower_20_2",
        "bb_width_20_2",
        "bb_percent_b_20_2",
        "williams_r_14",
        "slow_k_14_3",
        "slow_d_14_3",
        "ichimoku_tenkan",
        "ichimoku_kijun",
        "ichimoku_senkou_a",
        "ichimoku_senkou_b",
        "ichimoku_chikou",
        "atr_14",
        "atrp_14",
        "adx_14",
        "plus_di_14",
        "minus_di_14",
        "cci_20",
        "cci_signal_10",
        "obv",
        "cmf_20",
        "adi",
        "mfi_14",
        "roc_6",
        "roc_12",
        "roc_20",
        "realized_vol_10d",
        "realized_vol_20d",
        "realized_vol_60d",
        "realized_vol_20d_ann",
        "parkinson_vol_20",
        "gk_vol_20",
        "vwap_d",
        "rvol_20",
        "beta_60",
        "corr_mkt_60",
        "is_validated",
        "quality_score",
    ]

    if pdf.empty:
        return pd.DataFrame(columns=output_columns)

    pdf = pdf.sort_values("date").reset_index(drop=True).copy()
    pdf["date"] = pd.to_datetime(pdf["date"])

    result = pd.DataFrame(
        {
            "symbol": pdf["symbol"],
            "date": pdf["date"],
            "ds": pdf["ds"].astype(str),
        }
    )

    open_ = pd.to_numeric(pdf.get("open"), errors="coerce")
    high = pd.to_numeric(pdf.get("high"), errors="coerce")
    low = pd.to_numeric(pdf.get("low"), errors="coerce")
    close = pd.to_numeric(pdf.get("close"), errors="coerce")
    volume = pd.to_numeric(pdf.get("volume"), errors="coerce")

    market_returns = _prepare_market_returns(pdf)

    log_returns = np.log(close / close.shift(1))

    result["sma_20"] = close.rolling(window=20, min_periods=20).mean()
    result["sma_60"] = close.rolling(window=60, min_periods=60).mean()
    result["sma_120"] = close.rolling(window=120, min_periods=120).mean()

    result["ema_20"] = _ema(close, 20)
    result["ema_60"] = _ema(close, 60)

    return_1d_decimal = close.pct_change()
    result["return_1d"] = return_1d_decimal * 100.0
    result["return_5d"] = (close / close.shift(5) - 1.0) * 100.0
    result["return_20d"] = (close / close.shift(20) - 1.0) * 100.0
    result["log_return_1d"] = log_returns

    envelope_mid = result["sma_20"]
    result["envelope_mid_20_3"] = envelope_mid
    result["envelope_upper_20_3"] = envelope_mid * 1.03
    result["envelope_lower_20_3"] = envelope_mid * 0.97

    rsi_14 = _rsi_wilder(close, 14)
    result["rsi_14"] = rsi_14
    result["rsi_ema6"] = _ema(rsi_14, 6)

    ema_12 = _ema(close, 12)
    ema_26 = _ema(close, 26)
    macd = ema_12 - ema_26
    macd_signal = _ema(macd, 9)
    macd_hist = macd - macd_signal

    result["macd_12_26"] = macd
    result["macd_signal_9"] = macd_signal
    result["macd_hist_12_26_9"] = macd_hist
    result["macd_pct_12_26"] = (macd / close.replace(0.0, np.nan)) * 100.0

    boll_upper, boll_mid, boll_lower = _bollinger(close, 20, 2.0)
    result["bollinger_upper_20_2"] = boll_upper
    result["bollinger_middle_20_2"] = boll_mid
    result["bollinger_lower_20_2"] = boll_lower

    band_width = (boll_upper - boll_lower) / boll_mid.replace(0.0, np.nan)
    result["bb_width_20_2"] = band_width * 100.0
    denom = (boll_upper - boll_lower).replace(0.0, np.nan)
    pct_b = (close - boll_lower) / denom
    result["bb_percent_b_20_2"] = pct_b

    result["williams_r_14"] = _williams_r(close, high, low, 14)

    slow_k, slow_d = _stochastic_kd(close, high, low, 14)
    result["slow_k_14_3"] = slow_k
    result["slow_d_14_3"] = slow_d

    conversion_line = (high.rolling(window=9, min_periods=9).max() + low.rolling(window=9, min_periods=9).min()) / 2.0
    base_line = (high.rolling(window=26, min_periods=26).max() + low.rolling(window=26, min_periods=26).min()) / 2.0
    leading_span_b_raw = (
        high.rolling(window=52, min_periods=52).max() + low.rolling(window=52, min_periods=52).min()
    ) / 2.0

    result["ichimoku_tenkan"] = conversion_line
    result["ichimoku_kijun"] = base_line
    result["ichimoku_senkou_a"] = ((conversion_line + base_line) / 2.0).shift(26)
    result["ichimoku_senkou_b"] = leading_span_b_raw.shift(26)
    result["ichimoku_chikou"] = close.shift(-26)

    atr_14 = _atr(high, low, close, 14)
    result["atr_14"] = atr_14
    result["atrp_14"] = (atr_14 / close.replace(0.0, np.nan)) * 100.0

    adx_14, plus_di_14, minus_di_14 = _directional_indicators(high, low, close, 14)
    result["adx_14"] = adx_14
    result["plus_di_14"] = plus_di_14
    result["minus_di_14"] = minus_di_14

    cci_20 = _commodity_channel_index(high, low, close, 20)
    result["cci_20"] = cci_20
    result["cci_signal_10"] = _ema(cci_20, 10)

    result["obv"] = _obv(close, volume)

    cmf_20, adi = _chaikin_money_flow(high, low, close, volume, 20)
    result["cmf_20"] = cmf_20
    result["adi"] = adi

    result["mfi_14"] = _money_flow_index(high, low, close, volume, 14)

    result["roc_6"] = _rate_of_change(close, 6)
    result["roc_12"] = _rate_of_change(close, 12)
    result["roc_20"] = _rate_of_change(close, 20)

    result["realized_vol_10d"] = _realized_volatility(log_returns, 10)
    result["realized_vol_20d"] = _realized_volatility(log_returns, 20)
    result["realized_vol_60d"] = _realized_volatility(log_returns, 60)
    result["realized_vol_20d_ann"] = _realized_volatility(log_returns, 20, annualize=True)

    result["parkinson_vol_20"] = _parkinson_volatility(high, low, 20)
    result["gk_vol_20"] = _garman_klass_volatility(open_, high, low, close, 20)

    vwap_col = None
    for candidate in ("vwap", "vwap_d"):
        if candidate in pdf.columns:
            vwap_col = pd.to_numeric(pdf[candidate], errors="coerce")
            break
    result["vwap_d"] = vwap_col

    avg_volume_20 = volume.rolling(window=20, min_periods=20).mean()
    result["rvol_20"] = volume / avg_volume_20.replace(0.0, np.nan)

    beta_60 = pd.Series(np.nan, index=result.index)
    corr_60 = pd.Series(np.nan, index=result.index)
    if market_returns is not None:
        returns_asset = return_1d_decimal
        returns_bench = market_returns
        cov = returns_asset.rolling(window=60, min_periods=30).cov(returns_bench)
        var = returns_bench.rolling(window=60, min_periods=30).var()
        beta_60 = cov / var.replace(0.0, np.nan)
        corr_60 = returns_asset.rolling(window=60, min_periods=30).corr(returns_bench)

    result["beta_60"] = beta_60
    result["corr_mkt_60"] = corr_60

    vwap_default = result["vwap_d"].fillna(np.nan)
    result["vwap_d"] = vwap_default

    violation_masks: Dict[str, pd.Series] = {}

    # Columns that must be zero or positive per table specification
    non_negative_fields = [
        "sma_20",
        "sma_60",
        "sma_120",
        "ema_20",
        "ema_60",
        "envelope_mid_20_3",
        "envelope_upper_20_3",
        "envelope_lower_20_3",
        "bollinger_middle_20_2",
        "bollinger_upper_20_2",
        "bollinger_lower_20_2",
        "ichimoku_tenkan",
        "ichimoku_kijun",
        "ichimoku_senkou_a",
        "ichimoku_senkou_b",
        "ichimoku_chikou",
        "atr_14",
        "atrp_14",
        "bb_width_20_2",
        "realized_vol_10d",
        "realized_vol_20d",
        "realized_vol_60d",
        "realized_vol_20d_ann",
        "parkinson_vol_20",
        "gk_vol_20",
        "rvol_20",
    ]
    for field in non_negative_fields:
        result[field], violation_masks[field] = _apply_non_negative(result[field])

    ranged_fields: Dict[str, Bounds] = {
        "rsi_14": Bounds(0.0, 100.0),
        "rsi_ema6": Bounds(0.0, 100.0),
        "slow_k_14_3": Bounds(0.0, 100.0),
        "slow_d_14_3": Bounds(0.0, 100.0),
        "williams_r_14": Bounds(-100.0, 0.0),
        "bb_percent_b_20_2": Bounds(0.0, 1.0),
        "adx_14": Bounds(0.0, 100.0),
        "plus_di_14": Bounds(0.0, 100.0),
        "minus_di_14": Bounds(0.0, 100.0),
        "cmf_20": Bounds(-1.0, 1.0),
        "mfi_14": Bounds(0.0, 100.0),
        "beta_60": Bounds(-3.0, 3.0),
        "corr_mkt_60": Bounds(-1.0, 1.0),
    }
    for field, bounds in ranged_fields.items():
        result[field], violation_masks[field] = _apply_bounds(result[field], bounds)

    boolean_masks = [mask for mask in violation_masks.values() if not mask.empty]
    if boolean_masks:
        violations_df = pd.concat(boolean_masks, axis=1)
        violation_count = violations_df.sum(axis=1)
        total_checks = violations_df.shape[1]
    else:
        violation_count = pd.Series(0, index=result.index)
        total_checks = 1

    valid_fraction = 1.0 - (violation_count / total_checks)
    quality_score = (valid_fraction * 100.0).round().clip(lower=0.0, upper=100.0)

    result["is_validated"] = (violation_count == 0).astype(np.uint8)
    result["quality_score"] = quality_score.fillna(0.0).astype(np.uint8)

    result = result.reindex(columns=output_columns)
    return result
