"""Glue job to compact raw objects into curated parquet partitions.

The job aggregates all raw objects for a given domain/table/day and writes a
single optimized Parquet partition (`ds=YYYY-MM-DD`). It keeps the raw data in
place and focuses on producing a curated dataset for downstream transforms.
"""

from __future__ import annotations

import sys
from typing import Tuple

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession

from shared.paths import build_curated_layer_path

_MEGABYTE: int = 1024 * 1024


def _sanitize_prefix(prefix: str) -> str:
    """Return a normalized S3 key prefix without leading/trailing slashes."""

    return prefix.strip("/")


def _resolve_raw_location(raw_bucket: str, raw_prefix: str, ds: str) -> Tuple[str, str]:
    """Return (s3_uri, key_prefix) for raw objects of the partition."""

    normalized = _sanitize_prefix(raw_prefix)
    year, month, day = ds.split("-")
    if all(token in normalized for token in ("year=", "month=", "day=")):
        key_prefix = f"{normalized.rstrip('/')}/"
    else:
        base = f"{normalized}/" if normalized else ""
        key_prefix = f"{base}year={year}/month={month}/day={day}/"

    s3_uri = f"s3://{raw_bucket}/{key_prefix}"
    return s3_uri, key_prefix


def _raw_objects_exist(s3_client, bucket: str, key_prefix: str) -> bool:
    """Return True when at least one raw object exists for the prefix."""

    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=key_prefix, MaxKeys=1)
    return int(response.get("KeyCount", 0)) > 0


def _read_raw_dataframe(spark: SparkSession, file_type: str, path: str) -> DataFrame:
    """Read raw objects using the requested serialization format."""

    fmt = file_type.lower()
    if fmt == "json":
        return spark.read.json(path)
    if fmt == "csv":
        return spark.read.option("header", True).option("inferSchema", True).csv(path)
    if fmt == "parquet":
        return spark.read.parquet(path)

    raise ValueError(f"Unsupported file_type '{file_type}' for compaction job")


def _configure_spark(spark: SparkSession, codec: str, target_file_mb: int) -> None:
    """Apply Spark settings for deterministic parquet output."""

    spark.conf.set("spark.sql.parquet.compression.codec", codec)
    # Align output file sizing with requested MB target (avoid single huge file).
    target_bytes = max(32, target_file_mb) * _MEGABYTE
    spark.conf.set("spark.sql.files.maxPartitionBytes", str(target_bytes))
    spark.conf.set("spark.sql.shuffle.partitions", "1")


def main() -> None:
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "raw_bucket",
            "raw_prefix",
            "compacted_bucket",
            "domain",
            "table_name",
            "ds",
            "file_type",
            "codec",
            "target_file_mb",
            "interval",
            "data_source",
            "layer",
        ],
    )

    interval: str = args.get("interval", "")
    data_source: str = args.get("data_source", "")
    if not interval or not data_source:
        raise ValueError("Compaction job requires --interval and --data_source")

    spark_context = SparkContext()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    raw_path, raw_key_prefix = _resolve_raw_location(args["raw_bucket"], args["raw_prefix"], args["ds"])
    compacted_path = build_curated_layer_path(
        domain=args["domain"],
        table=args["table_name"],
        interval=interval,
        data_source=data_source,
        ds=args["ds"],
        layer=args.get("layer", "compacted"),
    )
    compacted_path = f"s3://{args['compacted_bucket']}/{compacted_path}"

    s3_client = boto3.client("s3")
    if not _raw_objects_exist(s3_client, args["raw_bucket"], raw_key_prefix):
        print(f"No raw objects found for prefix '{raw_key_prefix}', skipping compaction")
        job.commit()
        return

    target_file_mb = int(args.get("target_file_mb", "256"))
    _configure_spark(spark, args["codec"], target_file_mb)

    df = _read_raw_dataframe(spark, args["file_type"], raw_path)
    record_count = df.count()
    if record_count == 0:
        print(f"Raw dataset empty for {raw_key_prefix}, skipping output write")
        job.commit()
        return

    df = df.coalesce(1)

    print(f"Compacting {record_count} records from {raw_path} to {compacted_path}")
    (df.write.mode("overwrite").format("parquet").option("compression", args["codec"]).save(compacted_path))

    job.commit()


if __name__ == "__main__":
    main()
