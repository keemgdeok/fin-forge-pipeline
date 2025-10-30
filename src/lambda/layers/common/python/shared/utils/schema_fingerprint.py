"""Schema fingerprint utilities for transform pipelines.

These helpers compute a stable schema fingerprint, validate S3 URIs, and
upload the fingerprint JSON to S3. They are intentionally Spark-agnostic:
callers provide a list of column descriptors: [{"name": str, "type": str}].
"""

from __future__ import annotations

import json
import hashlib
from typing import Any, Dict, Sequence, Tuple


def stable_hash(obj: Dict[str, Any]) -> str:
    """Return a deterministic SHA-256 hash of a JSON-serializable object."""
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def build_fingerprint(*, columns: Sequence[Dict[str, str]], codec: str) -> Dict[str, Any]:
    """Build a schema fingerprint payload.

    - columns: sequence of {"name": str, "type": str}
    - codec: compression codec used in Parquet writes
    """
    cols_list = [
        {"name": str(c.get("name", "")), "type": str(c.get("type", ""))} for c in columns if c and c.get("name")
    ]
    payload = {"columns": cols_list, "codec": str(codec)}
    payload["hash"] = stable_hash({"columns": cols_list})
    return payload


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    """Parse an s3://bucket/key URI into (bucket, key)."""
    if not isinstance(uri, str) or not uri.startswith("s3://"):
        raise ValueError("schema_fingerprint_s3_uri must be s3://<bucket>/<key>")
    bucket_key = uri[5:]
    if "/" not in bucket_key:
        raise ValueError("schema_fingerprint_s3_uri must include bucket and key path")
    bucket, key = bucket_key.split("/", 1)
    if not bucket or not key:
        raise ValueError("schema_fingerprint_s3_uri must include bucket and key path")
    return bucket, key


def put_fingerprint_s3(
    *,
    s3_client: Any,
    bucket: str,
    key: str,
    fingerprint: Dict[str, Any],
) -> None:
    """Upload fingerprint JSON to S3 using the provided boto3 client."""
    body = json.dumps(fingerprint).encode("utf-8")
    s3_client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
