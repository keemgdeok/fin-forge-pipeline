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


# Simple DQ: non-empty dataset
count = df.limit(1).count()
if count == 0:
    raise RuntimeError("NO_RAW_DATA: No records found for ds")

# Minimal critical DQ rules (generic, domain-agnostic)
# - If column 'symbol' exists -> not null
# - If column 'price' exists -> >= 0
violations: list[str] = []
if "symbol" in df.columns:
    if df.filter(F.col("symbol").isNull()).limit(1).count() > 0:
        violations.append("null symbol present")
if "price" in df.columns:
    if df.filter(F.col("price") < F.lit(0)).limit(1).count() > 0:
        violations.append("negative price present")

if violations:
    _write_quarantine_and_fail(df, "; ".join(violations))

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
