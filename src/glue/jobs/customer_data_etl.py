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
    ],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

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


# ==== COMPREHENSIVE DATA QUALITY VALIDATION ====
# Now includes ALL DQ logic previously handled by separate Lambda functions
# Optimized for 1GB daily batch processing

print(f"Starting DQ validation for {args['ds']} - total records to validate: {df.count()}")

# Critical DQ Rule 1: Non-empty dataset
record_count = df.count()
if record_count == 0:
    raise RuntimeError("NO_RAW_DATA: No records found for ds")

print(f"✓ Dataset non-empty: {record_count} records")

# Critical DQ Rule 2: Domain-specific field validation
violations: list[str] = []
critical_violations = 0
warning_violations = 0

# Generic business rules (adapt based on your domain)
if "symbol" in df.columns:
    null_symbol_count = df.filter(F.col("symbol").isNull()).count()
    if null_symbol_count > 0:
        critical_violations += null_symbol_count
        violations.append(f"null symbol present ({null_symbol_count} records)")

if "price" in df.columns:
    negative_price_count = df.filter(F.col("price") < F.lit(0)).count()
    if negative_price_count > 0:
        critical_violations += negative_price_count
        violations.append(f"negative price present ({negative_price_count} records)")

# Critical DQ Rule 3: Duplicate detection (for 1GB data, full scan is acceptable)
if "id" in df.columns or "symbol" in df.columns:
    key_col = "id" if "id" in df.columns else "symbol"
    duplicate_count = df.groupBy(key_col).count().filter(F.col("count") > 1).count()
    if duplicate_count > 0:
        warning_violations += duplicate_count
        violations.append(f"duplicate {key_col} detected ({duplicate_count} groups)")

# Critical DQ Rule 4: Data completeness check (for 1GB batch)
expected_min_records = int(args.get("expected_min_records", "100"))  # Configurable
if record_count < expected_min_records:
    violations.append(f"low record count: {record_count} < {expected_min_records}")
    warning_violations += 1

# Critical DQ Rule 5: Schema consistency (basic type validation)
for field in df.schema.fields:
    if field.name in ["price", "amount", "value"] and "decimal" not in field.dataType.simpleString().lower():
        violations.append(f"invalid numeric type for {field.name}: {field.dataType.simpleString()}")
        warning_violations += 1

print(f"DQ Summary: Critical={critical_violations}, Warnings={warning_violations}, Total Records={record_count}")

# DQ Decision Logic (1GB batch optimized thresholds)
critical_error_rate = (critical_violations / record_count) * 100 if record_count > 0 else 0
max_critical_error_rate = float(args.get("max_critical_error_rate", "5.0"))  # 5% threshold

if violations:
    violation_summary = "; ".join(violations)
    print(f"DQ Issues Found: {violation_summary}")
    print(f"Critical Error Rate: {critical_error_rate:.2f}% (threshold: {max_critical_error_rate}%)")

    # For 1GB daily batch: fail only if critical error rate exceeds threshold
    if critical_error_rate > max_critical_error_rate:
        _write_quarantine_and_fail(
            df, f"Critical error rate {critical_error_rate:.2f}% > {max_critical_error_rate}%: {violation_summary}"
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
fingerprint = {
    "columns": cols,
    "codec": args["codec"],
    "hash": _stable_hash({"columns": cols}),
}

# Persist fingerprint to artifacts bucket (latest.json)
s3 = boto3.client("s3")
fp_uri = args["schema_fingerprint_s3_uri"]
if not fp_uri.startswith("s3://"):
    raise ValueError("schema_fingerprint_s3_uri must be s3://...")

_bucket_key = fp_uri[5:]
_bucket = _bucket_key.split("/", 1)[0]
_key = _bucket_key.split("/", 1)[1]
s3.put_object(
    Bucket=_bucket,
    Key=_key,
    Body=json.dumps(fingerprint).encode("utf-8"),
    ContentType="application/json",
)

job.commit()
