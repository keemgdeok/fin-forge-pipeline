from __future__ import annotations

from datetime import date, timedelta
from typing import Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from glue.lib.spark_indicators import OPTIONAL_DOUBLE_COLUMNS, REQUIRED_BASE_COLUMNS


def standardize_price_dataframe(df: DataFrame) -> DataFrame:
    """Cast required columns to deterministic types and add a ``date`` column."""

    for column_name in REQUIRED_BASE_COLUMNS:
        spark_type = "string" if column_name in {"symbol", "ds"} else "double"
        df = df.withColumn(column_name, F.col(column_name).cast(spark_type))

    for optional_column in OPTIONAL_DOUBLE_COLUMNS:
        if optional_column in df.columns:
            df = df.withColumn(optional_column, F.col(optional_column).cast("double"))

    return df.withColumn("date", F.to_date("ds"))


def combine_price_history(
    *,
    fresh_df: DataFrame,
    state_df: Optional[DataFrame],
    dedupe_keys: Tuple[str, str] = ("symbol", "ds"),
) -> DataFrame:
    """Merge fresh and state price data while preferring freshly loaded rows."""

    if state_df is None:
        return fresh_df

    labelled_fresh = fresh_df.withColumn("_source_rank", F.lit(0))
    labelled_state = state_df.withColumn("_source_rank", F.lit(1))

    combined = labelled_fresh.unionByName(labelled_state, allowMissingColumns=True)
    window = Window.partitionBy(*dedupe_keys).orderBy(F.col("_source_rank"))
    combined = combined.withColumn("_rn", F.row_number().over(window))
    return combined.where(F.col("_rn") == F.lit(1)).drop("_source_rank", "_rn")


def clamp_history_window(df: DataFrame, *, start: date, end: date) -> DataFrame:
    """Return history limited to ``[start, end]`` inclusive."""

    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")
    return df.where((F.col("ds") >= F.lit(start_str)) & (F.col("ds") <= F.lit(end_str)))


def retention_start_for_window(target_ds: date, window_days: int) -> date:
    """Compute the inclusive lower bound date for a rolling window."""

    safe_days = max(1, window_days)
    return target_ds - timedelta(days=safe_days - 1)
