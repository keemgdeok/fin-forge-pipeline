import sys
import json
import hashlib
import boto3  # type: ignore
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# Optional import of shared fingerprint utilities. The Glue job environment may
# not package the Common Layer; if unavailable, fall back to local implementations.
try:  # pragma: no cover - optional path
    from shared.utils.schema_fingerprint import (
        build_fingerprint as _build_fp,
        parse_s3_uri as _parse_s3_uri,
        put_fingerprint_s3 as _put_fp_s3,
    )

    _HAVE_SHARED_FP = True
except Exception:  # pragma: no cover - test environments without layer
    _HAVE_SHARED_FP = False

# Optional import of shared DQ engine (runtime-agnostic). The Glue job will
# prefer using the engine when available; otherwise it falls back to inline DQ.
try:  # pragma: no cover - optional path
    from shared.dq.engine import DQMetrics, DQConfig, evaluate as dq_evaluate

    _HAVE_DQ_ENGINE = True
except Exception:  # pragma: no cover - test environments without layer
    _HAVE_DQ_ENGINE = False


def _stable_hash(obj: dict) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "raw_bucket",
        "raw_prefix",
        "curated_bucket",
        "curated_prefix",
        "environment",
        "schema_fingerprint_s3_uri",
        "codec",
        "target_file_mb",
        "ds",
        "file_type",
        "expected_min_records",
        "max_critical_error_rate",
    ],
)


sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

"""Glue Job: Customer Data ETL with Data Quality validation.

Reads raw partition for ds, validates via shared DQ engine when present, then
persists curated data and schema fingerprint artifacts.
"""

# Read raw partition for ds
raw_path = f"s3://{args['raw_bucket']}/{args['raw_prefix']}ingestion_date={args['ds']}/"
ft = args["file_type"].lower()
if ft == "json":
    df = spark.read.json(raw_path)
elif ft == "csv":
    df = spark.read.option("header", True).option("inferSchema", True).csv(raw_path)
else:
    df = spark.read.parquet(raw_path)


def _write_quarantine_and_fail(df_in: DataFrame, reason: str) -> None:
    """Write dataset to quarantine path and raise DQ failure."""
    curated_path_base = f"s3://{args['curated_bucket']}/{args['curated_prefix']}"
    quarantine_path = f"{curated_path_base}quarantine/ds={args['ds']}"
    # Write as Parquet with same compression settings
    spark.conf.set("spark.sql.parquet.compression.codec", args["codec"])  # zstd
    df_in.coalesce(1).write.mode("overwrite").format("parquet").save(quarantine_path)
    raise RuntimeError(f"DQ_FAILED: {reason}")


"""==== COMPREHENSIVE DATA QUALITY VALIDATION ===="""
df = df.cache()

# Critical DQ Rule 1: Non-empty dataset
record_count = df.count()
if record_count == 0:
    raise RuntimeError("NO_RAW_DATA: No records found for ds")
print(f"Starting DQ validation for {args['ds']} - total records: {record_count}")

# Collect metrics (single-pass style where possible)
null_symbol_count = 0
negative_price_count = 0
duplicate_groups = 0
invalid_numeric_type_issues = 0

# Compute multiple counts in a single aggregation action where possible
agg_exprs = []
if "symbol" in df.columns:
    agg_exprs.append(F.sum(F.when(F.col("symbol").isNull(), F.lit(1)).otherwise(F.lit(0))).alias("__null_symbol_count"))
if "price" in df.columns:
    agg_exprs.append(
        F.sum(F.when(F.col("price") < F.lit(0), F.lit(1)).otherwise(F.lit(0))).alias("__negative_price_count")
    )
if agg_exprs:
    agg_row = df.agg(*agg_exprs).collect()[0]
    if "__null_symbol_count" in agg_row.asDict():
        null_symbol_count = int(agg_row["__null_symbol_count"])  # type: ignore[index]
    if "__negative_price_count" in agg_row.asDict():
        negative_price_count = int(agg_row["__negative_price_count"])  # type: ignore[index]

key_col = None
if "id" in df.columns:
    key_col = "id"
elif "symbol" in df.columns:
    key_col = "symbol"

if key_col:
    duplicate_groups = df.groupBy(key_col).count().filter(F.col("count") > 1).count()

for field in df.schema.fields:
    if field.name in ["price", "amount", "value"] and "decimal" not in field.dataType.simpleString().lower():
        invalid_numeric_type_issues += 1

expected_min_records = int(args.get("expected_min_records", "100"))
max_critical_error_rate = float(args.get("max_critical_error_rate", "5.0"))

if _HAVE_DQ_ENGINE:
    cfg = DQConfig(
        expected_min_records=expected_min_records,
        max_critical_error_rate=max_critical_error_rate,
    )
    metrics = DQMetrics(
        record_count=record_count,
        null_symbol_count=null_symbol_count,
        negative_price_count=negative_price_count,
        duplicate_key_groups=duplicate_groups,
        invalid_numeric_type_issues=invalid_numeric_type_issues,
    )
    result = dq_evaluate(metrics, cfg)
    print(
        "DQ Summary: "
        f"Critical={result.critical_violations}, "
        f"Warnings={result.warning_violations}, "
        f"CER={result.critical_error_rate:.2f}%"
    )
    if result.action == "quarantine":
        _write_quarantine_and_fail(df, "; ".join(result.messages))
    else:
        if result.messages:
            print("; ".join(result.messages))
        print("Proceeding with curated write")
else:
    # Inline fallback if engine layer is not available
    violations: list[str] = []
    critical_violations = 0
    warning_violations = 0

    if null_symbol_count > 0:
        critical_violations += null_symbol_count
        violations.append(f"null symbol present ({null_symbol_count} records)")

    if negative_price_count > 0:
        critical_violations += negative_price_count
        violations.append(f"negative price present ({negative_price_count} records)")

    if duplicate_groups > 0:
        warning_violations += duplicate_groups
        col_name = key_col or "key"
        violations.append(f"duplicate {col_name} detected ({duplicate_groups} groups)")

    if record_count < expected_min_records:
        violations.append(f"low record count: {record_count} < {expected_min_records}")
        warning_violations += 1

    if invalid_numeric_type_issues > 0:
        violations.append(f"invalid numeric type issues: {invalid_numeric_type_issues}")
        warning_violations += invalid_numeric_type_issues

    print(f"DQ Summary: Critical={critical_violations}, Warnings={warning_violations}, Total Records={record_count}")
    critical_error_rate = (critical_violations / record_count) * 100 if record_count > 0 else 0
    if violations:
        violation_summary = "; ".join(violations)
        print(f"DQ Issues Found: {violation_summary}")
        print(f"Critical Error Rate: {critical_error_rate:.2f}% (threshold: {max_critical_error_rate}%)")
        if critical_error_rate > max_critical_error_rate:
            _write_quarantine_and_fail(
                df,
                f"Critical error rate {critical_error_rate:.2f}% > {max_critical_error_rate}%: {violation_summary}",
            )
        else:
            print("⚠️  DQ issues within acceptable threshold, proceeding with warnings")
    else:
        print("✅ All DQ validations passed successfully")

# Add ds column, write to curated partitioned path in Parquet
spark.conf.set("spark.sql.parquet.compression.codec", args["codec"])  # zstd
df_out = df.withColumn("ds", F.lit(args["ds"]))
curated_path = f"s3://{args['curated_bucket']}/{args['curated_prefix']}"
df_out.coalesce(1).write.mode("append").partitionBy("ds").format("parquet").save(curated_path)

# Produce schema fingerprint from DataFrame schema
cols = [{"name": f.name, "type": f.dataType.simpleString()} for f in df_out.schema.fields if f.name != "ds"]
if _HAVE_SHARED_FP:
    fingerprint = _build_fp(columns=cols, codec=args["codec"])
else:
    fingerprint = {
        "columns": cols,
        "codec": args["codec"],
        "hash": _stable_hash({"columns": cols}),
    }

# Persist fingerprint to artifacts bucket (latest.json) and preserve previous.json
s3 = boto3.client("s3")
fp_uri = args["schema_fingerprint_s3_uri"]
if not fp_uri.startswith("s3://"):
    raise ValueError("schema_fingerprint_s3_uri must be s3://...")

if _HAVE_SHARED_FP:
    _bucket, _key = _parse_s3_uri(fp_uri)
else:
    _bucket_key = fp_uri[5:]
    _bucket = _bucket_key.split("/", 1)[0]
    _key = _bucket_key.split("/", 1)[1]

# Try to preserve previous fingerprint if exists
try:
    prev_obj = s3.get_object(Bucket=_bucket, Key=_key)
    prev_body = prev_obj["Body"].read()
    prev_key = _key.rsplit("/", 1)[0] + "/previous.json"
    s3.put_object(Bucket=_bucket, Key=prev_key, Body=prev_body, ContentType="application/json")
except Exception:
    # No previous fingerprint available; skip preservation
    pass

# Write latest fingerprint
if _HAVE_SHARED_FP:
    _put_fp_s3(s3_client=s3, bucket=_bucket, key=_key, fingerprint=fingerprint)
else:
    s3.put_object(
        Bucket=_bucket,
        Key=_key,
        Body=json.dumps(fingerprint).encode("utf-8"),
        ContentType="application/json",
    )

job.commit()
