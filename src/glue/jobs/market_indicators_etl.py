from __future__ import annotations

import sys
import json
import hashlib
from datetime import datetime, timedelta
from typing import Any, Dict

import boto3  # type: ignore
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Optional imports from shared layer for schema fingerprint
try:  # pragma: no cover
    from shared.utils.schema_fingerprint import (
        build_fingerprint as _build_fp,
        parse_s3_uri as _parse_s3_uri,
        put_fingerprint_s3 as _put_fp_s3,
    )

    _HAVE_SHARED_FP = True
except Exception:  # pragma: no cover
    _HAVE_SHARED_FP = False

# Import indicators library (bundled with job code)
from glue.lib.indicators import compute_indicators_pandas  # type: ignore


def _stable_hash(obj: Dict[str, Any]) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "environment",
        "prices_curated_bucket",
        "prices_prefix",
        "output_bucket",
        "output_prefix",
        "schema_fingerprint_s3_uri",
        "codec",
        "target_file_mb",
        "ds",
        "lookback_days",
    ],
)


def _get_opt_arg(name: str, default: str) -> str:
    """Parse optional CLI arg from sys.argv like Glue would pass ("--name value")."""
    flag = f"--{name}"
    if flag in sys.argv:
        i = sys.argv.index(flag)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return default


sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

"""Glue Job: Indicators ETL (Curated prices -> Curated indicators).

Reads curated prices for [ds - lookback_days + 1, ds], computes technical
indicators per (symbol, date), and writes the ds partition to curated indicators.
"""

# Spark write config
spark.conf.set("spark.sql.parquet.compression.codec", args["codec"])  # zstd

# Date window
ds_str = str(args["ds"]).strip()
lookback_days = int(args.get("lookback_days", "252"))
ds_dt = datetime.strptime(ds_str, "%Y-%m-%d").date()
start_dt = ds_dt - timedelta(days=max(lookback_days - 1, 0))


def _build_path(scheme: str, bucket: str, prefix: str) -> str:
    if scheme == "file":
        # Ensure file URI has exactly 3 slashes before absolute path
        if bucket.startswith("/"):
            base = f"file://{bucket}"
        else:
            base = f"file:///{bucket}"
        return f"{base.rstrip('/')}/{prefix.lstrip('/')}"
    return f"{scheme}://{bucket}/{prefix}"


uri_scheme = _get_opt_arg("uri_scheme", "s3")

# Read curated prices (partitioned by ds)
prices_path = _build_path(uri_scheme, args["prices_curated_bucket"], args["prices_prefix"])
prices_df = (
    spark.read.format("parquet")
    .load(prices_path)
    .select(
        F.col("symbol").cast(T.StringType()).alias("symbol"),
        F.col("ds").cast(T.StringType()).alias("ds"),
        F.col("open").cast(T.DoubleType()).alias("open"),
        F.col("high").cast(T.DoubleType()).alias("high"),
        F.col("low").cast(T.DoubleType()).alias("low"),
        F.col("close").cast(T.DoubleType()).alias("close"),
        F.col("volume").cast(T.DoubleType()).alias("volume"),
    )
    .where((F.col("ds") >= F.lit(start_dt.strftime("%Y-%m-%d"))) & (F.col("ds") <= F.lit(ds_str)))
    .withColumn("date", F.to_date("ds"))
)

# Minimal input DQ
record_count = prices_df.count()
if record_count == 0:
    raise RuntimeError("NO_INPUT_DATA: No curated prices found in lookback window")

null_pk = prices_df.filter(F.col("symbol").isNull() | F.col("date").isNull()).limit(1).count()
if null_pk > 0:
    raise RuntimeError("DQ_FAILED: Null symbol/date detected in curated prices")


# Schema for applyInPandas output
out_schema = T.StructType(
    [
        T.StructField("symbol", T.StringType(), False),
        T.StructField("date", T.DateType(), False),
        T.StructField("ds", T.StringType(), False),
        T.StructField("open", T.DoubleType(), True),
        T.StructField("high", T.DoubleType(), True),
        T.StructField("low", T.DoubleType(), True),
        T.StructField("close", T.DoubleType(), True),
        T.StructField("volume", T.DoubleType(), True),
        T.StructField("sma_20", T.DoubleType(), True),
        T.StructField("sma_50", T.DoubleType(), True),
        T.StructField("sma_200", T.DoubleType(), True),
        T.StructField("ema_12", T.DoubleType(), True),
        T.StructField("ema_26", T.DoubleType(), True),
        T.StructField("rsi_14", T.DoubleType(), True),
        T.StructField("macd", T.DoubleType(), True),
        T.StructField("macd_signal", T.DoubleType(), True),
        T.StructField("macd_histogram", T.DoubleType(), True),
        T.StructField("bollinger_upper", T.DoubleType(), True),
        T.StructField("bollinger_middle", T.DoubleType(), True),
        T.StructField("bollinger_lower", T.DoubleType(), True),
        T.StructField("williams_r", T.DoubleType(), True),
        T.StructField("stochastic_k", T.DoubleType(), True),
        T.StructField("stochastic_d", T.DoubleType(), True),
        T.StructField("atr_14", T.DoubleType(), True),
        T.StructField("adx_14", T.DoubleType(), True),
        T.StructField("obv", T.DoubleType(), True),
        T.StructField("realized_volatility_20d", T.DoubleType(), True),
    ]
)


def _compute_group(pdf: Any) -> Any:
    # Ensure expected columns and types
    pdf = pdf[
        [
            "symbol",
            "date",
            "ds",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
    ].copy()
    return compute_indicators_pandas(pdf)


indicators_df = prices_df.groupby("symbol").applyInPandas(_compute_group, schema=out_schema)

# Keep only target ds for output
ind_ds_df = indicators_df.where(F.col("ds") == F.lit(ds_str))

# DQ: duplicates on (date, symbol)
dups = ind_ds_df.groupBy("date", "symbol").count().where(F.col("count") > F.lit(1)).limit(1).count()
if dups > 0:
    # quarantine
    curated_base = f"s3://{args['output_bucket']}/{args['output_prefix']}"
    quarantine_path = f"{curated_base}quarantine/ds={ds_str}"
    ind_ds_df.coalesce(1).write.mode("overwrite").format("parquet").save(quarantine_path)
    raise RuntimeError("DQ_FAILED: duplicate (date,symbol) detected in indicators output")

# Cast/round to target Decimal scales


def _round(col_name: str, scale: int) -> F.Column:
    return F.round(F.col(col_name), scale)


def _to_decimal(col_name: str, precision: int, scale: int) -> F.Column:
    return F.col(col_name).cast(T.DecimalType(precision=precision, scale=scale))


ind_out = (
    ind_ds_df.withColumn("date", F.to_date("date"))
    # Round first for human readability and stable decimals
    .withColumn("sma_20", _round("sma_20", 4))
    .withColumn("sma_50", _round("sma_50", 4))
    .withColumn("sma_200", _round("sma_200", 4))
    .withColumn("ema_12", _round("ema_12", 4))
    .withColumn("ema_26", _round("ema_26", 4))
    .withColumn("rsi_14", _round("rsi_14", 2))
    .withColumn("macd", _round("macd", 6))
    .withColumn("macd_signal", _round("macd_signal", 6))
    .withColumn("macd_histogram", _round("macd_histogram", 6))
    .withColumn("bollinger_upper", _round("bollinger_upper", 4))
    .withColumn("bollinger_middle", _round("bollinger_middle", 4))
    .withColumn("bollinger_lower", _round("bollinger_lower", 4))
    .withColumn("williams_r", _round("williams_r", 2))
    .withColumn("stochastic_k", _round("stochastic_k", 2))
    .withColumn("stochastic_d", _round("stochastic_d", 2))
    .withColumn("atr_14", _round("atr_14", 4))
    .withColumn("adx_14", _round("adx_14", 2))
    .withColumn("obv", F.col("obv"))
    .withColumn("realized_volatility_20d", _round("realized_volatility_20d", 4))
    # Cast to target DECIMAL/INTEGER where applicable
    .withColumn("sma_20", _to_decimal("sma_20", 12, 4))
    .withColumn("sma_50", _to_decimal("sma_50", 12, 4))
    .withColumn("sma_200", _to_decimal("sma_200", 12, 4))
    .withColumn("ema_12", _to_decimal("ema_12", 12, 4))
    .withColumn("ema_26", _to_decimal("ema_26", 12, 4))
    .withColumn("rsi_14", _to_decimal("rsi_14", 5, 2))
    .withColumn("macd", _to_decimal("macd", 10, 6))
    .withColumn("macd_signal", _to_decimal("macd_signal", 10, 6))
    .withColumn("macd_histogram", _to_decimal("macd_histogram", 10, 6))
    .withColumn("bollinger_upper", _to_decimal("bollinger_upper", 12, 4))
    .withColumn("bollinger_middle", _to_decimal("bollinger_middle", 12, 4))
    .withColumn("bollinger_lower", _to_decimal("bollinger_lower", 12, 4))
    .withColumn("williams_r", _to_decimal("williams_r", 6, 2))
    .withColumn("stochastic_k", _to_decimal("stochastic_k", 5, 2))
    .withColumn("stochastic_d", _to_decimal("stochastic_d", 5, 2))
    .withColumn("atr_14", _to_decimal("atr_14", 8, 4))
    .withColumn("adx_14", _to_decimal("adx_14", 5, 2))
    .withColumn("obv", F.col("obv").cast(T.LongType()))
    .withColumn("realized_volatility_20d", _to_decimal("realized_volatility_20d", 6, 4))
)

# Write curated indicators
out_path = _build_path(uri_scheme, args["output_bucket"], args["output_prefix"])
ind_out.coalesce(1).write.mode("append").partitionBy("ds").format("parquet").save(out_path)

# Build schema fingerprint (exclude ds)
cols = [{"name": f.name, "type": f.dataType.simpleString()} for f in ind_out.schema.fields if f.name != "ds"]

s3 = boto3.client("s3")
fp_uri = args["schema_fingerprint_s3_uri"]
if not fp_uri.startswith("s3://"):
    raise ValueError("schema_fingerprint_s3_uri must be s3://...")

if _HAVE_SHARED_FP:
    fingerprint = _build_fp(columns=cols, codec=args["codec"])
    bucket, key = _parse_s3_uri(fp_uri)
else:
    fingerprint = {"columns": cols, "codec": args["codec"], "hash": _stable_hash({"columns": cols})}
    bucket_key = fp_uri[5:]
    bucket = bucket_key.split("/", 1)[0]
    key = bucket_key.split("/", 1)[1]

# Preserve previous
try:
    prev_obj = s3.get_object(Bucket=bucket, Key=key)
    prev_body = prev_obj["Body"].read()
    prev_key = key.rsplit("/", 1)[0] + "/previous.json"
    s3.put_object(Bucket=bucket, Key=prev_key, Body=prev_body, ContentType="application/json")
except Exception:
    pass

if _HAVE_SHARED_FP:
    _put_fp_s3(s3_client=s3, bucket=bucket, key=key, fingerprint=fingerprint)
else:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(fingerprint).encode("utf-8"),
        ContentType="application/json",
    )

job.commit()
