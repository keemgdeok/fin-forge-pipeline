from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

import boto3  # type: ignore
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.utils import AnalysisException

from glue.lib.incremental_state import (
    clamp_history_window,
    combine_price_history,
    retention_start_for_window,
    standardize_price_dataframe,
)
from shared.paths import build_curated_interval_prefix, build_curated_layer_path, build_curated_state_path

# Optional imports from shared layer for schema fingerprint utilities
try:  # pragma: no cover
    from shared.utils.schema_fingerprint import (
        build_fingerprint as _build_fp,
        parse_s3_uri as _parse_s3_uri,
        put_fingerprint_s3 as _put_fp_s3,
    )

    _HAVE_SHARED_FP = True
except Exception:  # pragma: no cover
    _HAVE_SHARED_FP = False

from glue.lib.spark_indicators import (
    DECIMAL_10_6_COLUMNS,
    DECIMAL_12_4_COLUMNS,
    DECIMAL_8_4_COLUMNS,
    FLOAT_COLUMNS,
    INDICATOR_COLUMNS,
    INT64_COLUMNS,
    REQUIRED_BASE_COLUMNS,
    SHORT_COLUMNS,
    compute_indicators_spark,
)


def _stable_hash(obj: Dict[str, Any]) -> str:
    payload = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _get_opt_arg(name: str, default: str) -> str:
    flag = f"--{name}"
    if flag in sys.argv:
        idx = sys.argv.index(flag)
        if idx + 1 < len(sys.argv):
            return sys.argv[idx + 1]
    return default


def _build_path(scheme: str, bucket: str, prefix: str) -> str:
    if scheme == "file":
        base = f"file://{bucket}" if bucket.startswith("/") else f"file:///{bucket}"
        return f"{base.rstrip('/')}/{prefix.lstrip('/')}"
    return f"{scheme}://{bucket}/{prefix}"


def _cast_decimal(df: DataFrame, columns: Tuple[str, ...], precision: int, scale: int) -> DataFrame:
    for column_name in columns:
        df = df.withColumn(column_name, F.round(F.col(column_name), scale))
        df = df.withColumn(column_name, F.col(column_name).cast(T.DecimalType(precision=precision, scale=scale)))
    return df


def _cast_float(df: DataFrame, columns: Tuple[str, ...], scale: int = 6) -> DataFrame:
    for column_name in columns:
        df = df.withColumn(column_name, F.round(F.col(column_name), scale).cast(T.FloatType()))
    return df


def _cast_int64(df: DataFrame, columns: Tuple[str, ...]) -> DataFrame:
    for column_name in columns:
        df = df.withColumn(column_name, F.col(column_name).cast(T.LongType()))
    return df


def _cast_short(df: DataFrame, columns: Tuple[str, ...]) -> DataFrame:
    for column_name in columns:
        df = df.withColumn(column_name, F.col(column_name).cast(T.ShortType()))
    return df


DEFAULT_OUTPUT_PARTITIONS = 4


def _determine_output_partitions(_: SparkContext, desired_output_partitions: int) -> int:
    return max(1, desired_output_partitions)


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "environment",
        "prices_curated_bucket",
        "output_bucket",
        "schema_fingerprint_s3_uri",
        "codec",
        "target_file_mb",
        "ds",
        "lookback_days",
        "interval",
        "data_source",
        "domain",
        "table_name",
        "prices_layer",
        "output_layer",
        "output_partitions",
    ],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)
desired_output_partitions = int(args.get("output_partitions", DEFAULT_OUTPUT_PARTITIONS))
output_partitions = _determine_output_partitions(sc, desired_output_partitions)
spark.conf.set("spark.sql.shuffle.partitions", str(output_partitions))

spark.conf.set("spark.sql.parquet.compression.codec", args["codec"])

uri_scheme = _get_opt_arg("uri_scheme", "s3")
ds_str = str(args["ds"]).strip()
lookback_days = int(args.get("lookback_days", "252"))
ds_dt = datetime.strptime(ds_str, "%Y-%m-%d").date()
now_utc = datetime.now(timezone.utc)
batch_id_value = int(abs(now_utc.timestamp()) * 1_000_000)
created_at_value = now_utc
updated_at_value = now_utc

interval = str(args.get("interval") or "").strip()
data_source = str(args.get("data_source") or "").strip()
domain = str(args.get("domain") or "").strip()
table_name = str(args.get("table_name") or "").strip()
prices_layer = str(args.get("prices_layer") or "adjusted").strip()
output_layer = str(args.get("output_layer") or "technical_indicator").strip()
state_bucket = str(args.get("state_bucket") or args.get("output_bucket") or "").strip()
state_layer = str(args.get("state_layer") or "state").strip()
state_snapshot = str(args.get("state_snapshot") or "current").strip()
state_window_days = max(1, int(str(args.get("state_window_days", lookback_days))))
state_enabled = bool(state_bucket and state_layer and state_snapshot)
state_key: str | None = None
state_path: str | None = None

if not interval or not domain or not table_name:
    raise ValueError("Indicators job requires domain, table_name, and interval arguments")

interval_prefix = build_curated_interval_prefix(
    domain=domain,
    table=table_name,
    interval=interval,
    data_source=data_source or None,
)

prices_path = _build_path(uri_scheme, args["prices_curated_bucket"], interval_prefix)
raw_prices_df = spark.read.format("parquet").load(prices_path)
state_df: DataFrame | None = None
if state_enabled:
    state_key = build_curated_state_path(
        domain=domain,
        table=table_name,
        interval=interval,
        data_source=data_source or None,
        state_layer=state_layer,
        snapshot=state_snapshot,
    )
    state_path = _build_path(uri_scheme, state_bucket, state_key)
    try:
        candidate_state_df = spark.read.format("parquet").load(state_path)
        if candidate_state_df.head(1):
            state_df = standardize_price_dataframe(candidate_state_df)
    except AnalysisException:
        state_df = None

history_days_for_read = max(1, lookback_days if state_df is None else 1)
read_start_dt = retention_start_for_window(ds_dt, history_days_for_read)

prices_df = raw_prices_df.where(
    (F.col("ds") >= F.lit(read_start_dt.strftime("%Y-%m-%d")))
    & (F.col("ds") <= F.lit(ds_str))
    & (F.col("layer") == F.lit(prices_layer))
)

missing_columns = [col for col in REQUIRED_BASE_COLUMNS if col not in prices_df.columns]
if missing_columns:
    raise RuntimeError(f"MISSING_COLUMNS: Curated prices missing required fields {missing_columns}")

prices_df = standardize_price_dataframe(prices_df)

combined_prices_df = combine_price_history(fresh_df=prices_df, state_df=state_df)

history_days_for_compute = max(lookback_days, state_window_days if state_enabled else lookback_days)
history_start_dt = retention_start_for_window(ds_dt, history_days_for_compute)

if "layer" in combined_prices_df.columns:
    combined_prices_df = combined_prices_df.where(F.col("layer") == F.lit(prices_layer))

combined_prices_df = clamp_history_window(combined_prices_df, start=history_start_dt, end=ds_dt)
combined_prices_df = combined_prices_df.cache()

record_count = combined_prices_df.count()
if record_count == 0:
    raise RuntimeError("NO_INPUT_DATA: No curated prices found in lookback window")

target_record_count = combined_prices_df.filter(F.col("ds") == F.lit(ds_str)).limit(1).count()
if target_record_count == 0:
    raise RuntimeError(f"NO_TARGET_DATA: No curated prices found for ds {ds_str}")

null_pk = combined_prices_df.filter(F.col("symbol").isNull() | F.col("date").isNull()).limit(1).count()
if null_pk > 0:
    raise RuntimeError("DQ_FAILED: Null symbol/date detected in curated prices")

history_prices_df = combined_prices_df.repartition(output_partitions, "symbol")

indicators_df = compute_indicators_spark(history_prices_df)
indicators_df = indicators_df.withColumn("batch_id", F.lit(batch_id_value))
indicators_df = indicators_df.withColumn("created_at", F.lit(created_at_value))
indicators_df = indicators_df.withColumn("updated_at", F.lit(updated_at_value))
indicators_df = indicators_df.withColumn("is_validated", F.col("is_validated").cast(T.IntegerType()))
indicators_df = indicators_df.withColumn("quality_score", F.col("quality_score").cast(T.IntegerType()))
ind_ds_df = indicators_df.where(F.col("ds") == F.lit(ds_str))

duplicate_check = ind_ds_df.groupBy("date", "symbol").count().where(F.col("count") > F.lit(1)).limit(1).count()
if duplicate_check > 0:
    quarantine_key = build_curated_layer_path(
        domain=domain,
        table=table_name,
        interval=interval,
        data_source=data_source or None,
        ds=ds_str,
        layer="quarantine",
    )
    quarantine_path = _build_path(uri_scheme, args["output_bucket"], quarantine_key)
    ind_ds_df.repartition(output_partitions).write.mode("overwrite").format("parquet").save(quarantine_path)
    raise RuntimeError("DQ_FAILED: duplicate (date,symbol) detected in indicators output")

ind_out = ind_ds_df.withColumn("date", F.to_date("date"))
ind_out = _cast_decimal(ind_out, DECIMAL_12_4_COLUMNS, precision=12, scale=4)
ind_out = _cast_decimal(ind_out, DECIMAL_10_6_COLUMNS, precision=10, scale=6)
ind_out = _cast_decimal(ind_out, DECIMAL_8_4_COLUMNS, precision=8, scale=4)
ind_out = _cast_float(ind_out, FLOAT_COLUMNS, scale=6)
ind_out = _cast_int64(ind_out, INT64_COLUMNS)
ind_out = _cast_short(ind_out, SHORT_COLUMNS)
ind_out = ind_out.withColumn("batch_id", F.col("batch_id").cast(T.LongType()))
ind_out = ind_out.withColumn("created_at", F.to_timestamp(F.col("created_at")))
ind_out = ind_out.withColumn("updated_at", F.to_timestamp(F.col("updated_at")))
ind_out = ind_out.withColumn("layer", F.lit(output_layer))

ordered_output_columns = INDICATOR_COLUMNS + ["layer", "batch_id", "created_at", "updated_at"]
ind_out = ind_out.select(*ordered_output_columns)

output_key = build_curated_layer_path(
    domain=domain,
    table=table_name,
    interval=interval,
    data_source=data_source or None,
    ds=ds_str,
    layer=output_layer,
)
out_path = _build_path(uri_scheme, args["output_bucket"], output_key)
ind_out.repartition(output_partitions).write.mode("overwrite").format("parquet").save(out_path)

if state_enabled and state_path:
    retention_start_dt = retention_start_for_window(ds_dt, state_window_days)
    state_output_df = clamp_history_window(combined_prices_df, start=retention_start_dt, end=ds_dt)
    if state_output_df.limit(1).count() == 0:
        raise RuntimeError("STATE_UPDATE_FAILED: No rows available to persist in state snapshot")
    state_output_df.orderBy("symbol", "ds").coalesce(1).write.mode("overwrite").format("parquet").save(state_path)

cols = [
    {"name": field.name, "type": field.dataType.simpleString()} for field in ind_out.schema.fields if field.name != "ds"
]

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

try:
    previous_object = s3.get_object(Bucket=bucket, Key=key)
    previous_body = previous_object["Body"].read()
    previous_key = key.rsplit("/", 1)[0] + "/previous.json"
    s3.put_object(Bucket=bucket, Key=previous_key, Body=previous_body, ContentType="application/json")
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

combined_prices_df.unpersist()
job.commit()
OUTPUT_PARTITIONS = 4
