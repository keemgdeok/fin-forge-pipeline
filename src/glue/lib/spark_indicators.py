from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Sequence, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window, WindowSpec


INDICATOR_COLUMNS: List[str] = [
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

DECIMAL_12_4_COLUMNS: Tuple[str, ...] = (
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
    "vwap_d",
)

DECIMAL_10_6_COLUMNS: Tuple[str, ...] = (
    "macd_12_26",
    "macd_signal_9",
    "macd_hist_12_26_9",
)

DECIMAL_8_4_COLUMNS: Tuple[str, ...] = ("atr_14",)

FLOAT_COLUMNS: Tuple[str, ...] = (
    "return_1d",
    "return_5d",
    "return_20d",
    "log_return_1d",
    "macd_pct_12_26",
    "bb_width_20_2",
    "bb_percent_b_20_2",
    "williams_r_14",
    "slow_k_14_3",
    "slow_d_14_3",
    "atrp_14",
    "adx_14",
    "plus_di_14",
    "minus_di_14",
    "cci_20",
    "cci_signal_10",
    "cmf_20",
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
    "rvol_20",
    "beta_60",
    "corr_mkt_60",
)

INT64_COLUMNS: Tuple[str, ...] = ("obv", "adi")
SHORT_COLUMNS: Tuple[str, ...] = ("is_validated", "quality_score")
OPTIONAL_DOUBLE_COLUMNS: Tuple[str, ...] = (
    "market_close",
    "benchmark_close",
    "market_return",
    "market_return_1d",
    "benchmark_return",
    "vwap",
    "vwap_d",
)
REQUIRED_BASE_COLUMNS: Tuple[str, ...] = ("symbol", "ds", "open", "high", "low", "close", "volume")


NON_NEGATIVE_FIELDS: Tuple[str, ...] = (
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
)


RANGED_FIELDS: Dict[str, Tuple[float | None, float | None]] = {
    "rsi_14": (0.0, 100.0),
    "rsi_ema6": (0.0, 100.0),
    "slow_k_14_3": (0.0, 100.0),
    "slow_d_14_3": (0.0, 100.0),
    "williams_r_14": (-100.0, 0.0),
    "bb_percent_b_20_2": (0.0, 1.0),
    "adx_14": (0.0, 100.0),
    "plus_di_14": (0.0, 100.0),
    "minus_di_14": (0.0, 100.0),
    "cmf_20": (-1.0, 1.0),
    "mfi_14": (0.0, 100.0),
    "beta_60": (-3.0, 3.0),
    "corr_mkt_60": (-1.0, 1.0),
}


@dataclass(frozen=True)
class IndicatorConfig:
    """Configuration required to compute indicators."""

    partition_cols: Sequence[str] = ("symbol",)
    order_col: str = "date"


def _rolling_mean(column: Column, window: WindowSpec, min_periods: int) -> Column:
    count_expr = F.count(column).over(window)
    mean_expr = F.avg(column).over(window)
    return F.when(count_expr >= F.lit(min_periods), mean_expr)


def _rolling_sum(column: Column, window: WindowSpec, min_periods: int) -> Column:
    count_expr = F.count(column).over(window)
    sum_expr = F.sum(column).over(window)
    return F.when(count_expr >= F.lit(min_periods), sum_expr)


def _rolling_stddev(column: Column, window: WindowSpec, min_periods: int, sample: bool = False) -> Column:
    count_expr = F.count(column).over(window)
    std_func = F.stddev_samp if sample else F.stddev_pop
    std_expr = std_func(column).over(window)
    return F.when(count_expr >= F.lit(min_periods), std_expr)


def _rolling_min(column: Column, window: WindowSpec, min_periods: int) -> Column:
    count_expr = F.count(column).over(window)
    min_expr = F.min(column).over(window)
    return F.when(count_expr >= F.lit(min_periods), min_expr)


def _rolling_max(column: Column, window: WindowSpec, min_periods: int) -> Column:
    count_expr = F.count(column).over(window)
    max_expr = F.max(column).over(window)
    return F.when(count_expr >= F.lit(min_periods), max_expr)


def _ewm(column: Column, window: WindowSpec, alpha: float) -> Column:
    alpha_lit = F.lit(alpha)
    beta_lit = F.lit(1.0 - alpha)
    return F.aggregate(
        F.collect_list(column).over(window),
        F.lit(None).cast("double"),
        lambda acc, x: F.when(
            x.isNull(),
            acc,
        ).otherwise(F.when(acc.isNull(), x).otherwise(alpha_lit * x + beta_lit * acc)),
    )


def _prepare_market_returns(df: DataFrame, order_window: WindowSpec, partition_window: WindowSpec) -> DataFrame:
    decimal_candidates = ("market_return", "market_return_1d", "benchmark_return")
    price_candidates = ("market_close", "benchmark_close", "sp500_close", "spy_close")

    for candidate in decimal_candidates:
        if candidate in df.columns:
            df = df.withColumn("_market_candidate", F.col(candidate).cast("double"))
            max_abs = F.max(F.abs(F.col("_market_candidate"))).over(partition_window)
            df = df.withColumn(
                "market_return_series",
                F.when(
                    max_abs > F.lit(10.0),
                    F.col("_market_candidate") / F.lit(100.0),
                ).otherwise(F.col("_market_candidate")),
            )
            return df.drop("_market_candidate")

    for candidate in price_candidates:
        if candidate in df.columns:
            df = df.withColumn("_market_candidate", F.col(candidate).cast("double"))
            df = df.withColumn(
                "market_return_series",
                F.col("_market_candidate") / F.lag("_market_candidate").over(order_window) - F.lit(1.0),
            )
            return df.drop("_market_candidate")

    return df.withColumn("market_return_series", F.lit(None).cast("double"))


def _apply_bounds(df: DataFrame, field: str, lower: float | None, upper: float | None) -> DataFrame:
    value = F.col(field)
    lower_cond = value >= F.lit(lower) if lower is not None else F.lit(True)
    upper_cond = value <= F.lit(upper) if upper is not None else F.lit(True)
    valid_mask = (lower_cond & upper_cond) | value.isNull()
    violation_col = F.when(valid_mask, F.lit(False)).otherwise(F.lit(True))
    adjusted_value = F.when(valid_mask, value).otherwise(F.lit(None))
    df = df.withColumn(f"__viol_{field}", violation_col)
    df = df.withColumn(field, adjusted_value)
    return df


def _apply_non_negative(df: DataFrame, field: str) -> DataFrame:
    value = F.col(field)
    valid_mask = (value >= F.lit(0.0)) | value.isNull()
    violation_col = F.when(valid_mask, F.lit(False)).otherwise(F.lit(True))
    adjusted_value = F.when(valid_mask, value).otherwise(F.lit(None))
    df = df.withColumn(field, adjusted_value)
    df = df.withColumn(f"__viol_{field}", violation_col)
    return df


def compute_indicators_spark(df: DataFrame, config: IndicatorConfig | None = None) -> DataFrame:
    cfg = config or IndicatorConfig()

    partition_window = Window.partitionBy(*cfg.partition_cols)
    order_window = Window.partitionBy(*cfg.partition_cols).orderBy(cfg.order_col)
    cumulative_window = order_window.rowsBetween(Window.unboundedPreceding, 0)

    window_3 = order_window.rowsBetween(-(3 - 1), 0)
    window_9 = order_window.rowsBetween(-(9 - 1), 0)
    window_10 = order_window.rowsBetween(-(10 - 1), 0)
    window_14 = order_window.rowsBetween(-(14 - 1), 0)
    window_20 = order_window.rowsBetween(-(20 - 1), 0)
    window_26 = order_window.rowsBetween(-(26 - 1), 0)
    window_52 = order_window.rowsBetween(-(52 - 1), 0)
    window_60 = order_window.rowsBetween(-(60 - 1), 0)
    window_120 = order_window.rowsBetween(-(120 - 1), 0)

    df = _prepare_market_returns(df, order_window, partition_window)

    prev_close = F.lag("close").over(order_window)
    prev_close_5 = F.lag("close", 5).over(order_window)
    prev_close_20 = F.lag("close", 20).over(order_window)
    prev_close_6 = F.lag("close", 6).over(order_window)
    prev_close_12 = F.lag("close", 12).over(order_window)

    df = df.withColumn(
        "return_1d_decimal",
        F.when(prev_close != 0.0, F.col("close") / prev_close - F.lit(1.0)),
    )
    df = df.withColumn("return_1d", F.col("return_1d_decimal") * F.lit(100.0))
    df = df.withColumn(
        "return_5d",
        F.when(prev_close_5 != 0.0, (F.col("close") / prev_close_5 - F.lit(1.0)) * F.lit(100.0)),
    )
    df = df.withColumn(
        "return_20d",
        F.when(prev_close_20 != 0.0, (F.col("close") / prev_close_20 - F.lit(1.0)) * F.lit(100.0)),
    )
    df = df.withColumn(
        "log_return_1d",
        F.when(prev_close != 0.0, F.log(F.col("close") / prev_close)),
    )

    df = df.withColumn("sma_20", _rolling_mean(F.col("close"), window_20, 20))
    df = df.withColumn("sma_60", _rolling_mean(F.col("close"), window_60, 60))
    df = df.withColumn("sma_120", _rolling_mean(F.col("close"), window_120, 120))

    ema_20 = _ewm(F.col("close"), cumulative_window, 2.0 / (20.0 + 1.0))
    ema_60 = _ewm(F.col("close"), cumulative_window, 2.0 / (60.0 + 1.0))
    df = df.withColumn("ema_20", ema_20)
    df = df.withColumn("ema_60", ema_60)

    df = df.withColumn("envelope_mid_20_3", F.col("sma_20"))
    df = df.withColumn("envelope_upper_20_3", F.col("sma_20") * F.lit(1.03))
    df = df.withColumn("envelope_lower_20_3", F.col("sma_20") * F.lit(0.97))

    delta = F.col("close") - prev_close
    gain = F.when(
        delta.isNull(),
        F.lit(None),
    ).otherwise(F.when(delta > 0.0, delta).otherwise(F.lit(0.0)))
    loss = F.when(
        delta.isNull(),
        F.lit(None),
    ).otherwise(F.when(delta < 0.0, -delta).otherwise(F.lit(0.0)))
    df = df.withColumn("__gain", gain).withColumn("__loss", loss)

    avg_gain = _ewm(F.col("__gain"), cumulative_window, 1.0 / 14.0)
    avg_loss = _ewm(F.col("__loss"), cumulative_window, 1.0 / 14.0)
    df = df.withColumn("__avg_gain", avg_gain).withColumn("__avg_loss", avg_loss)

    rs = F.col("__avg_gain") / F.when(F.col("__avg_loss") == 0.0, F.lit(None)).otherwise(F.col("__avg_loss"))
    rsi = F.lit(100.0) - (F.lit(100.0) / (F.lit(1.0) + rs))
    rsi = F.when((F.col("__avg_loss") == 0.0) & (F.col("__avg_gain") > 0.0), F.lit(100.0)).otherwise(rsi)
    rsi = F.when((F.col("__avg_gain") == 0.0) & (F.col("__avg_loss") > 0.0), F.lit(0.0)).otherwise(rsi)
    rsi = F.when((F.col("__avg_gain") == 0.0) & (F.col("__avg_loss") == 0.0), F.lit(50.0)).otherwise(rsi)
    df = df.withColumn("rsi_14", rsi)
    df = df.withColumn("rsi_ema6", _ewm(F.col("rsi_14"), cumulative_window, 2.0 / (6.0 + 1.0)))

    ema_12 = _ewm(F.col("close"), cumulative_window, 2.0 / (12.0 + 1.0))
    ema_26 = _ewm(F.col("close"), cumulative_window, 2.0 / (26.0 + 1.0))
    df = df.withColumn("__ema_12", ema_12).withColumn("__ema_26", ema_26)
    df = df.withColumn("macd_12_26", F.col("__ema_12") - F.col("__ema_26"))
    df = df.withColumn("macd_signal_9", _ewm(F.col("macd_12_26"), cumulative_window, 2.0 / (9.0 + 1.0)))
    df = df.withColumn("macd_hist_12_26_9", F.col("macd_12_26") - F.col("macd_signal_9"))
    df = df.withColumn(
        "macd_pct_12_26",
        F.when(F.col("close") != 0.0, F.col("macd_12_26") / F.col("close") * F.lit(100.0)),
    )

    rolling_std_20 = _rolling_stddev(F.col("close"), window_20, 20, sample=False)
    df = df.withColumn("bollinger_middle_20_2", F.col("sma_20"))
    df = df.withColumn("bollinger_upper_20_2", F.col("sma_20") + rolling_std_20 * F.lit(2.0))
    df = df.withColumn("bollinger_lower_20_2", F.col("sma_20") - rolling_std_20 * F.lit(2.0))

    band_width = (F.col("bollinger_upper_20_2") - F.col("bollinger_lower_20_2")) / F.col("sma_20")
    df = df.withColumn("bb_width_20_2", band_width * F.lit(100.0))
    bb_denom = F.col("bollinger_upper_20_2") - F.col("bollinger_lower_20_2")
    df = df.withColumn(
        "bb_percent_b_20_2",
        F.when(bb_denom != 0.0, (F.col("close") - F.col("bollinger_lower_20_2")) / bb_denom),
    )

    highest_high_14 = _rolling_max(F.col("high"), window_14, 14)
    lowest_low_14 = _rolling_min(F.col("low"), window_14, 14)
    williams_denom = highest_high_14 - lowest_low_14
    df = df.withColumn(
        "williams_r_14",
        F.when(
            williams_denom != 0.0,
            -F.lit(100.0) * (highest_high_14 - F.col("close")) / williams_denom,
        ),
    )

    df = df.withColumn(
        "slow_k_14_3",
        F.when(
            williams_denom != 0.0,
            F.lit(100.0) * (F.col("close") - lowest_low_14) / williams_denom,
        ),
    )
    df = df.withColumn("slow_d_14_3", _rolling_mean(F.col("slow_k_14_3"), window_3, 3))

    conversion_line = (_rolling_max(F.col("high"), window_9, 9) + _rolling_min(F.col("low"), window_9, 9)) / F.lit(2.0)
    base_line = (_rolling_max(F.col("high"), window_26, 26) + _rolling_min(F.col("low"), window_26, 26)) / F.lit(2.0)
    leading_span_b_raw = (
        _rolling_max(F.col("high"), window_52, 52) + _rolling_min(F.col("low"), window_52, 52)
    ) / F.lit(2.0)

    df = df.withColumn("ichimoku_tenkan", conversion_line)
    df = df.withColumn("ichimoku_kijun", base_line)
    df = df.withColumn("__senkou_a_raw", (conversion_line + base_line) / F.lit(2.0))
    df = df.withColumn("ichimoku_senkou_a", F.lag("__senkou_a_raw", 26).over(order_window))
    df = df.withColumn("__senkou_b_raw", leading_span_b_raw)
    df = df.withColumn("ichimoku_senkou_b", F.lag("__senkou_b_raw", 26).over(order_window))
    df = df.withColumn("ichimoku_chikou", F.lag("close", -26).over(order_window))

    true_range = F.greatest(
        F.abs(F.col("high") - F.col("low")),
        F.abs(F.col("high") - prev_close),
        F.abs(F.col("low") - prev_close),
    )
    df = df.withColumn("__true_range", true_range)
    atr = _ewm(F.col("__true_range"), cumulative_window, 1.0 / 14.0)
    df = df.withColumn("atr_14", atr)
    df = df.withColumn(
        "atrp_14",
        F.when(F.col("close") != 0.0, F.col("atr_14") / F.col("close") * F.lit(100.0)),
    )

    up_move = F.col("high") - F.lag("high").over(order_window)
    down_move = F.lag("low").over(order_window) - F.col("low")
    plus_dm = F.when((up_move > down_move) & (up_move > 0.0), up_move).otherwise(F.lit(0.0))
    minus_dm = F.when((down_move > up_move) & (down_move > 0.0), down_move).otherwise(F.lit(0.0))
    df = df.withColumn("__plus_dm", plus_dm).withColumn("__minus_dm", minus_dm)

    tr_smooth = _ewm(F.col("__true_range"), cumulative_window, 1.0 / 14.0)
    plus_di = (
        F.lit(100.0)
        * _ewm(F.col("__plus_dm"), cumulative_window, 1.0 / 14.0)
        / F.when(tr_smooth == 0.0, F.lit(None)).otherwise(tr_smooth)
    )
    minus_di = (
        F.lit(100.0)
        * _ewm(F.col("__minus_dm"), cumulative_window, 1.0 / 14.0)
        / F.when(tr_smooth == 0.0, F.lit(None)).otherwise(tr_smooth)
    )
    dx = (
        F.lit(100.0)
        * F.abs(plus_di - minus_di)
        / F.when(plus_di + minus_di == 0.0, F.lit(None)).otherwise(plus_di + minus_di)
    )
    adx = _ewm(dx, cumulative_window, 1.0 / 14.0)

    df = df.withColumn("adx_14", adx)
    df = df.withColumn("plus_di_14", plus_di)
    df = df.withColumn("minus_di_14", minus_di)

    typical_price = (F.col("high") + F.col("low") + F.col("close")) / F.lit(3.0)
    df = df.withColumn("__typical_price", typical_price)
    sma_tp = _rolling_mean(F.col("__typical_price"), window_20, 20)
    df = df.withColumn("__sma_tp", sma_tp)

    typical_list = F.collect_list(F.col("__typical_price")).over(window_20)
    count_tp = F.count(F.col("__typical_price")).over(window_20)
    mad_sum = F.aggregate(
        typical_list,
        F.lit(0.0),
        lambda acc, x: F.when(x.isNull(), acc).otherwise(acc + F.abs(x - F.col("__sma_tp"))),
    )
    df = df.withColumn("__count_tp", count_tp)
    df = df.withColumn(
        "__mad_dev",
        F.when(F.col("__count_tp") >= 20, mad_sum / F.col("__count_tp")),
    )
    denom = F.lit(0.015) * F.col("__mad_dev")
    df = df.withColumn(
        "cci_20",
        F.when(denom != 0.0, (F.col("__typical_price") - F.col("__sma_tp")) / denom),
    )
    df = df.withColumn("cci_signal_10", _ewm(F.col("cci_20"), cumulative_window, 2.0 / (10.0 + 1.0)))

    direction = F.signum(F.coalesce(F.col("close") - prev_close, F.lit(0.0)))
    df = df.withColumn("obv", F.sum(direction * F.col("volume")).over(cumulative_window))

    high_low_range = F.col("high") - F.col("low")
    multiplier = F.when(
        high_low_range != 0.0,
        ((F.col("close") - F.col("low")) - (F.col("high") - F.col("close"))) / high_low_range,
    ).otherwise(F.lit(0.0))
    money_flow_volume = multiplier * F.coalesce(F.col("volume"), F.lit(0.0))
    df = df.withColumn("__money_flow_volume", money_flow_volume)
    flow_sum_20 = _rolling_sum(F.col("__money_flow_volume"), window_20, 20)
    volume_sum_20 = _rolling_sum(F.col("volume"), window_20, 20)
    df = df.withColumn(
        "cmf_20",
        flow_sum_20 / F.when(volume_sum_20 == 0.0, F.lit(None)).otherwise(volume_sum_20),
    )
    df = df.withColumn("adi", F.sum(F.col("__money_flow_volume")).over(cumulative_window))

    typical_price = F.col("__typical_price")
    money_flow = typical_price * F.coalesce(F.col("volume"), F.lit(0.0))
    price_delta = typical_price - F.lag(typical_price).over(order_window)
    positive_flow = F.when(price_delta > 0.0, money_flow).otherwise(F.lit(0.0))
    negative_flow = F.when(price_delta < 0.0, -money_flow).otherwise(F.lit(0.0))
    pos_roll = _rolling_sum(positive_flow, window_14, 14)
    neg_roll = _rolling_sum(negative_flow, window_14, 14)
    money_ratio = pos_roll / F.when(neg_roll == 0.0, F.lit(None)).otherwise(neg_roll)
    mfi = F.lit(100.0) - (F.lit(100.0) / (F.lit(1.0) + money_ratio))
    mfi = F.when((neg_roll == 0.0) & (pos_roll > 0.0), F.lit(100.0)).otherwise(mfi)
    mfi = F.when((pos_roll == 0.0) & (neg_roll > 0.0), F.lit(0.0)).otherwise(mfi)
    mfi = F.when((pos_roll == 0.0) & (neg_roll == 0.0), F.lit(None)).otherwise(mfi)
    df = df.withColumn("mfi_14", mfi)

    df = df.withColumn(
        "roc_6",
        F.when(prev_close_6 != 0.0, (F.col("close") / prev_close_6 - F.lit(1.0)) * F.lit(100.0)),
    )
    df = df.withColumn(
        "roc_12",
        F.when(prev_close_12 != 0.0, (F.col("close") / prev_close_12 - F.lit(1.0)) * F.lit(100.0)),
    )
    df = df.withColumn(
        "roc_20",
        F.when(prev_close_20 != 0.0, (F.col("close") / prev_close_20 - F.lit(1.0)) * F.lit(100.0)),
    )

    log_returns = F.log(F.col("close") / prev_close)
    df = df.withColumn("__log_returns", log_returns)
    df = df.withColumn(
        "realized_vol_10d",
        _rolling_stddev(F.col("__log_returns"), window_10, 10, sample=True) * F.lit(100.0),
    )
    df = df.withColumn(
        "realized_vol_20d",
        _rolling_stddev(F.col("__log_returns"), window_20, 20, sample=True) * F.lit(100.0),
    )
    df = df.withColumn(
        "realized_vol_60d",
        _rolling_stddev(F.col("__log_returns"), window_60, 60, sample=True) * F.lit(100.0),
    )
    df = df.withColumn(
        "realized_vol_20d_ann",
        _rolling_stddev(F.col("__log_returns"), window_20, 20, sample=True) * F.sqrt(F.lit(252.0)) * F.lit(100.0),
    )

    hl_log = F.log(F.col("high") / F.col("low"))
    hl_sq = hl_log * hl_log
    mean_hl_sq = _rolling_mean(hl_sq, window_20, 20)
    df = df.withColumn(
        "parkinson_vol_20",
        F.sqrt(mean_hl_sq / (F.lit(4.0) * F.log(F.lit(2.0)))) * F.sqrt(F.lit(252.0)) * F.lit(100.0),
    )

    safe_open = F.when(F.col("open") == 0.0, F.lit(None)).otherwise(F.col("open"))
    log_hl = F.log(F.col("high") / F.col("low"))
    log_co = F.log(F.col("close") / safe_open)
    rs = F.lit(0.5) * log_hl * log_hl - (F.lit(2.0) * F.log(F.lit(2.0)) - F.lit(1.0)) * log_co * log_co
    rs_mean = _rolling_mean(rs, window_20, 20)
    df = df.withColumn("gk_vol_20", F.sqrt(rs_mean * F.lit(252.0)) * F.lit(100.0))

    if "vwap" in df.columns:
        df = df.withColumn("vwap_d", F.col("vwap").cast("double"))
    elif "vwap_d" in df.columns:
        df = df.withColumn("vwap_d", F.col("vwap_d").cast("double"))
    else:
        df = df.withColumn("vwap_d", F.lit(None).cast("double"))

    avg_volume_20 = _rolling_mean(F.col("volume"), window_20, 20)
    df = df.withColumn(
        "rvol_20",
        F.when(avg_volume_20 != 0.0, F.col("volume") / avg_volume_20),
    )

    market_returns = F.col("market_return_series")
    returns_asset = F.col("return_1d_decimal")

    count_60 = F.count(returns_asset).over(window_60)
    sum_asset = F.sum(returns_asset).over(window_60)
    sum_bench = F.sum(market_returns).over(window_60)
    sum_asset_sq = F.sum(returns_asset * returns_asset).over(window_60)
    sum_bench_sq = F.sum(market_returns * market_returns).over(window_60)
    sum_cross = F.sum(returns_asset * market_returns).over(window_60)

    df = df.withColumn("__count_60", count_60)
    df = df.withColumn("__sum_asset", sum_asset)
    df = df.withColumn("__sum_bench", sum_bench)
    df = df.withColumn("__sum_asset_sq", sum_asset_sq)
    df = df.withColumn("__sum_bench_sq", sum_bench_sq)
    df = df.withColumn("__sum_cross", sum_cross)

    denom = F.col("__count_60")
    safe_denom = F.when(denom > 0, denom).otherwise(F.lit(None))
    cov_sample = (F.col("__sum_cross") - (F.col("__sum_asset") * F.col("__sum_bench") / safe_denom)) / (
        safe_denom - F.lit(1.0)
    )
    cov_sample = F.when(F.col("__count_60") > 1, cov_sample)

    var_asset = (F.col("__sum_asset_sq") - (F.col("__sum_asset") * F.col("__sum_asset") / safe_denom)) / (
        safe_denom - F.lit(1.0)
    )
    var_asset = F.when(F.col("__count_60") > 1, var_asset)

    var_bench = (F.col("__sum_bench_sq") - (F.col("__sum_bench") * F.col("__sum_bench") / safe_denom)) / (
        safe_denom - F.lit(1.0)
    )
    var_bench = F.when(F.col("__count_60") > 1, var_bench)

    df = df.withColumn("__cov_sample", cov_sample)
    df = df.withColumn("__var_asset", var_asset)
    df = df.withColumn("__var_bench", var_bench)

    df = df.withColumn(
        "beta_60",
        F.when(
            F.col("__count_60") >= 30,
            F.col("__cov_sample") / F.col("__var_bench"),
        ),
    )

    std_asset = F.sqrt(F.col("__var_asset"))
    std_bench = F.sqrt(F.col("__var_bench"))
    df = df.withColumn("__std_asset", std_asset)
    df = df.withColumn("__std_bench", std_bench)
    df = df.withColumn(
        "corr_mkt_60",
        F.when(
            F.col("__count_60") >= 30,
            F.col("__cov_sample") / (F.col("__std_asset") * F.col("__std_bench")),
        ),
    )

    df = df.withColumn("symbol", F.col("symbol"))
    df = df.withColumn("date", F.col("date"))
    df = df.withColumn("ds", F.col("ds").cast("string"))

    for field in NON_NEGATIVE_FIELDS:
        df = _apply_non_negative(df, field)
    for field, bounds in RANGED_FIELDS.items():
        df = _apply_bounds(df, field, bounds[0], bounds[1])

    violation_names = [name for name in df.columns if name.startswith("__viol_")]
    if violation_names:
        violation_sum = sum(F.col(name).cast("int") for name in violation_names)
        total_checks = F.lit(len(violation_names))
        df = df.withColumn("is_validated", F.when(violation_sum == 0, F.lit(1)).otherwise(F.lit(0)))
        df = df.withColumn(
            "quality_score",
            F.round((F.lit(1.0) - violation_sum / total_checks) * F.lit(100.0)),
        )
        df = df.withColumn("__viol_sum", violation_sum)
        df = df.withColumn("__viol_total", total_checks)
    else:
        df = df.withColumn("is_validated", F.lit(1))
        df = df.withColumn("quality_score", F.lit(100))

    drop_columns = [name for name in df.columns if name.startswith("__")]
    drop_columns.append("market_return_series")

    df = df.drop(*drop_columns)
    return df.select(*INDICATOR_COLUMNS)
