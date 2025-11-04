from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from typing import Iterable

import pandas as pd
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from glue.lib.indicators import compute_indicators_pandas
from glue.lib.spark_indicators import INDICATOR_COLUMNS, compute_indicators_spark
from glue.lib.incremental_state import (
    clamp_history_window,
    combine_price_history,
    retention_start_for_window,
    standardize_price_dataframe,
)

_RUN_SPARK_TESTS = os.getenv("RUN_SPARK_TESTS", "0").lower() in {"1", "true", "yes"}


@pytest.fixture(scope="session")
def spark_session() -> Iterable[SparkSession]:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark = SparkSession.builder.master("local[2]").appName("unit-test-market-indicators").getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()


def _build_sample_rows(num_days: int = 120) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    start = datetime(2024, 1, 1)
    for offset in range(num_days):
        current = start + timedelta(days=offset)
        rows.append(
            {
                "symbol": "AAA" if offset % 2 == 0 else "BBB",
                "ds": current.strftime("%Y-%m-%d"),
                "date": current,
                "open": 100.0 + offset * 0.25,
                "high": 101.0 + offset * 0.3,
                "low": 99.0 + offset * 0.2,
                "close": 100.5 + offset * 0.27,
                "volume": 1_000 + offset * 7,
                "market_close": 3_000 + offset * 1.5,
                "vwap": 100.2 + offset * 0.24,
            }
        )
    return rows


@pytest.mark.skipif(not _RUN_SPARK_TESTS, reason="RUN_SPARK_TESTS is disabled")
def test_compute_indicators_matches_pandas(spark_session: SparkSession) -> None:
    rows = _build_sample_rows()
    pdf = pd.DataFrame(rows)

    spark_df = spark_session.createDataFrame(pdf)
    spark_result = compute_indicators_spark(spark_df).orderBy("symbol", "date").toPandas().reset_index(drop=True)

    pandas_frames: list[pd.DataFrame] = []
    pandas_input = pdf[["symbol", "date", "ds", "open", "high", "low", "close", "volume", "market_close", "vwap"]]
    for _, group in pandas_input.groupby("symbol", as_index=False):
        pandas_frames.append(compute_indicators_pandas(group.copy()))
    pandas_result = pd.concat(pandas_frames, ignore_index=True).sort_values(["symbol", "date"], ignore_index=True)

    for column in INDICATOR_COLUMNS:
        pandas_series = pandas_result[column]
        spark_series = spark_result[column]
        assert len(pandas_series) == len(spark_series), f"Column {column} length mismatch"

        for idx, (expected, actual) in enumerate(zip(pandas_series, spark_series, strict=True)):
            if pd.isna(expected) and pd.isna(actual):
                continue
            assert pd.isna(expected) == pd.isna(actual), f"{column} index {idx}: pandas={expected}, spark={actual}"
            if isinstance(expected, (float, int)) and isinstance(actual, (float, int)):
                assert actual == pytest.approx(expected, rel=1e-6, abs=1e-6), f"{column} index {idx}"
            else:
                assert actual == expected, f"{column} index {idx}"


@pytest.mark.skipif(not _RUN_SPARK_TESTS, reason="RUN_SPARK_TESTS is disabled")
def test_incremental_state_matches_full_history(spark_session: SparkSession) -> None:
    rows = _build_sample_rows(num_days=130)
    pdf = pd.DataFrame(rows)
    pdf["layer"] = "adjusted"

    spark_df = spark_session.createDataFrame(pdf)
    standardized = standardize_price_dataframe(spark_df)

    full_history = compute_indicators_spark(standardized.repartition(1, "symbol")).cache()

    target_ds = pdf["ds"].iloc[-1]
    target_date = datetime.strptime(target_ds, "%Y-%m-%d").date()
    previous_day = target_date - timedelta(days=1)

    full_target = (
        full_history.where(F.col("ds") == F.lit(target_ds)).orderBy("symbol", "date").toPandas().reset_index(drop=True)
    )

    state_window_days = 160
    state_retention_start = retention_start_for_window(previous_day, state_window_days)
    state_source = standardized.where(F.col("ds") <= F.lit(previous_day.strftime("%Y-%m-%d")))
    state_df = clamp_history_window(state_source, start=state_retention_start, end=previous_day)

    fresh_df = standardized.where(F.col("ds") == F.lit(target_ds))
    combined = combine_price_history(fresh_df=fresh_df, state_df=state_df)
    combined_window = clamp_history_window(
        combined,
        start=retention_start_for_window(target_date, state_window_days),
        end=target_date,
    )

    incremental_result = (
        compute_indicators_spark(combined_window.repartition(1, "symbol"))
        .where(F.col("ds") == F.lit(target_ds))
        .orderBy("symbol", "date")
        .toPandas()
        .reset_index(drop=True)
    )

    full_history.unpersist()

    assert len(full_target) == len(incremental_result)

    for column in INDICATOR_COLUMNS:
        expected_series = full_target[column]
        incremental_series = incremental_result[column]
        assert len(expected_series) == len(incremental_series), f"Column {column} length mismatch"

        for idx, (expected, actual) in enumerate(zip(expected_series, incremental_series, strict=True)):
            if pd.isna(expected) and pd.isna(actual):
                continue
            assert pd.isna(expected) == pd.isna(actual), f"{column} index {idx}: expected={expected}, actual={actual}"
            if isinstance(expected, (float, int)) and isinstance(actual, (float, int)):
                assert actual == pytest.approx(expected, rel=1e-6, abs=1e-6), f"{column} index {idx}"
            else:
                assert actual == expected, f"{column} index {idx}"
