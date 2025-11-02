"""Helpers for manifest discovery and chunk summary lifecycle."""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from shared.utils.logger import get_logger

CHUNK_SUMMARY_ROOT = "manifests/tmp"
_MANIFEST_DS_PATTERN = re.compile(r"year=(\d{4})/month=(\d{2})/day=(\d{2})/")
_MANIFEST_DS_LEGACY_PATTERN = re.compile(r"ds=(\d{4}-\d{2}-\d{2})")


@dataclass(frozen=True)
class ManifestEntry:
    """Descriptor for a manifest to be processed."""

    ds: str
    manifest_key: str
    source: str


def chunk_summary_prefix(batch_id: str) -> str:
    """Return the S3 prefix used to store chunk summary JSON blobs."""

    return f"{CHUNK_SUMMARY_ROOT}/{batch_id}/"


def persist_chunk_summary(
    *,
    raw_bucket: str,
    batch_id: str,
    partition_summaries: Sequence[Mapping[str, Any]],
    s3_client: Optional[BaseClient] = None,
    log: Optional[Any] = None,
) -> None:
    """Persist aggregated partition metadata to S3 for later manifest synthesis."""

    if not raw_bucket or not batch_id or not partition_summaries:
        return

    client = s3_client or boto3.client("s3")
    key = f"{chunk_summary_prefix(batch_id)}{uuid.uuid4()}.json"
    logger = log or get_logger(__name__)

    try:
        client.put_object(
            Bucket=raw_bucket,
            Key=key,
            Body=json.dumps(list(partition_summaries), ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )
        logger.debug(
            "Persisted chunk summary",
            extra={"batch_id": batch_id, "key": key},
        )
    except Exception:  # pragma: no cover - defensive logging only
        logger.exception(
            "Failed to persist chunk summary",
            extra={"batch_id": batch_id, "key": key},
        )


def load_chunk_summaries(
    *,
    raw_bucket: str,
    batch_id: str,
    s3_client: Optional[BaseClient] = None,
    log: Optional[Any] = None,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Load and merge chunk summaries previously emitted by ingestion workers."""

    if not raw_bucket or not batch_id:
        return [], []

    client = s3_client or boto3.client("s3")
    prefix = chunk_summary_prefix(batch_id)
    paginator = client.get_paginator("list_objects_v2")
    logger = log or get_logger(__name__)

    aggregated: Dict[str, Dict[str, Any]] = {}
    keys: List[str] = []

    try:
        for page in paginator.paginate(Bucket=raw_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj.get("Key")
                if not key:
                    continue
                keys.append(key)
                try:
                    response = client.get_object(Bucket=raw_bucket, Key=key)
                    data = json.loads(response["Body"].read().decode("utf-8"))
                except Exception:
                    logger.exception(
                        "Failed to load chunk summary",
                        extra={"batch_id": batch_id, "key": key},
                    )
                    continue

                for entry in data or []:
                    ds = entry.get("ds")
                    if not ds:
                        continue
                    bucket = aggregated.setdefault(
                        ds,
                        {
                            "raw_prefix": entry.get("raw_prefix", ""),
                            "objects": [],
                        },
                    )
                    bucket.setdefault("objects", []).extend(entry.get("objects", []))
    except Exception:  # pragma: no cover - defensive logging only
        logger.exception("Failed to enumerate chunk summaries", extra={"batch_id": batch_id})
        return [], keys

    combined = [
        {
            "ds": ds,
            "raw_prefix": values.get("raw_prefix", ""),
            "objects": values.get("objects", []),
        }
        for ds, values in aggregated.items()
    ]

    return combined, keys


def cleanup_chunk_summaries(
    *,
    raw_bucket: str,
    keys: Sequence[str],
    s3_client: Optional[BaseClient] = None,
    log: Optional[Any] = None,
) -> None:
    """Delete temporary chunk summary blobs once manifests are written."""

    if not raw_bucket or not keys:
        return

    client = s3_client or boto3.client("s3")
    logger = log or get_logger(__name__)

    for key in keys:
        try:
            client.delete_object(Bucket=raw_bucket, Key=key)
        except Exception:  # pragma: no cover - defensive logging only
            logger.exception(
                "Failed to delete chunk summary",
                extra={"bucket": raw_bucket, "key": key},
            )


def collect_manifest_entries(
    *,
    batch_id: str,
    raw_bucket: str,
    domain: str,
    table_name: str,
    interval: str,
    data_source: str,
    manifest_basename: str = "_batch",
    manifest_suffix: str = ".manifest.json",
    tracker_table: Optional[Any] = None,
    tracker_item: Optional[Mapping[str, Any]] = None,
    s3_client: Optional[BaseClient] = None,
    log: Optional[Any] = None,
) -> List[ManifestEntry]:
    """Collect manifest descriptors for a batch from DynamoDB or chunk summaries."""

    logger = log or get_logger(__name__)
    client = s3_client or boto3.client("s3")

    tracker: Optional[Mapping[str, Any]] = tracker_item
    if tracker is None and tracker_table is not None:
        try:
            response = tracker_table.get_item(Key={"pk": batch_id})
            tracker = response.get("Item")
        except ClientError as exc:  # pragma: no cover - surfaced via caller
            logger.exception("Failed to load batch tracker item", extra={"batch_id": batch_id})
            raise

    suffix = _normalize_manifest_suffix(manifest_suffix)
    basename = manifest_basename or "_batch"

    entries: Dict[str, ManifestEntry] = {}

    if tracker:
        for entry in _entries_from_tracker(
            tracker,
            domain=domain,
            table_name=table_name,
            interval=interval,
            data_source=data_source,
            manifest_basename=basename,
            manifest_suffix=suffix,
        ):
            entries.setdefault(entry.ds, entry)

    if not entries:
        # Primary fallback: derive manifest descriptors from chunk summaries persisted in S3.
        # Batch tracker items no longer accumulate partition_payload to stay below DynamoDB size limits.
        combined, _ = load_chunk_summaries(
            raw_bucket=raw_bucket,
            batch_id=batch_id,
            s3_client=client,
            log=logger,
        )
        for summary in combined:
            ds = str(summary.get("ds") or "").strip()
            if not ds:
                continue
            manifest_key = summary.get("manifest_key")
            if manifest_key:
                source = "chunk_summary"
            else:
                try:
                    manifest_key = _build_manifest_key(
                        ds=ds,
                        domain=domain,
                        table_name=table_name,
                        interval=interval,
                        data_source=data_source,
                        manifest_basename=basename,
                        manifest_suffix=suffix,
                    )
                except ValueError:
                    logger.warning(
                        "Unable to derive manifest key for chunk summary entry",
                        extra={"batch_id": batch_id, "ds": ds},
                    )
                    continue
                source = "chunk_summary"
            entries.setdefault(ds, ManifestEntry(ds=ds, manifest_key=manifest_key, source=source))

    ordered = sorted(entries.values(), key=lambda item: item.ds)
    return ordered


def _entries_from_tracker(
    tracker: Mapping[str, Any],
    *,
    domain: str,
    table_name: str,
    interval: str,
    data_source: str,
    manifest_basename: str,
    manifest_suffix: str,
) -> Iterable[ManifestEntry]:
    manifest_keys = tracker.get("manifest_keys")
    if isinstance(manifest_keys, Sequence):
        for key_raw in manifest_keys:
            key = str(key_raw or "").strip()
            if not key:
                continue
            ds = _extract_ds_from_manifest_key(key)
            if not ds:
                continue
            yield ManifestEntry(ds=ds, manifest_key=key, source="dynamodb")

    for summary in _iter_partition_summaries(tracker):
        ds = str(summary.get("ds") or "").strip()
        if not ds:
            continue
        manifest_key = str(summary.get("manifest_key") or "").strip()
        source = "dynamodb"
        if not manifest_key:
            try:
                manifest_key = _build_manifest_key(
                    ds=ds,
                    domain=domain,
                    table_name=table_name,
                    interval=interval,
                    data_source=data_source,
                    manifest_basename=manifest_basename,
                    manifest_suffix=manifest_suffix,
                )
                source = "computed"
            except ValueError:
                continue
        yield ManifestEntry(ds=ds, manifest_key=manifest_key, source=source)


def _iter_partition_summaries(tracker: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
    fields = [
        tracker.get("finalizing_payload", {}).get("partition_summaries"),
        tracker.get("combined_partition_summaries"),
        tracker.get("partition_payload"),
    ]
    for collection in fields:
        if isinstance(collection, Sequence):
            for entry in collection:
                if isinstance(entry, Mapping):
                    yield entry


def _normalize_manifest_suffix(suffix: str) -> str:
    value = (suffix or ".manifest.json").strip()
    if not value:
        value = ".manifest.json"
    if not value.startswith("."):
        value = f".{value}"
    return value


def _build_manifest_key(
    *,
    ds: str,
    domain: str,
    table_name: str,
    interval: str,
    data_source: str,
    manifest_basename: str,
    manifest_suffix: str,
) -> str:
    try:
        year, month, day = ds.split("-")
    except ValueError as exc:
        raise ValueError(f"Invalid ds value: {ds}") from exc

    return (
        f"{domain}/{table_name}/interval={interval}/"
        f"data_source={data_source}/year={year}/month={month}/day={day}/"
        f"{manifest_basename}{manifest_suffix}"
    )


def _extract_ds_from_manifest_key(manifest_key: str) -> Optional[str]:
    match = _MANIFEST_DS_PATTERN.search(manifest_key)
    if match:
        year, month, day = match.groups()
        return f"{year}-{month}-{day}"
    legacy = _MANIFEST_DS_LEGACY_PATTERN.search(manifest_key)
    if legacy:
        return legacy.group(1)
    return None
