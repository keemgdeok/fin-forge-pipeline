"""Schema Change Decider Lambda for Transform pipeline.

Input event (expects at least):
{
  "glue_args": {"--schema_fingerprint_s3_uri": "s3://.../_schema/latest.json", ...},
  "catalog_update": "on_schema_change|never|force"  # optional, overrides default
}

Env:
- CATALOG_UPDATE_DEFAULT: default policy (on_schema_change|never|force), default: on_schema_change

Output:
{ "shouldRunCrawler": true|false }
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict

import boto3


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    if not isinstance(uri, str) or not uri.startswith("s3://"):
        raise ValueError("schema_fingerprint_s3_uri must be s3://<bucket>/<key>")
    rest = uri[5:]
    if "/" not in rest:
        raise ValueError("schema_fingerprint_s3_uri must include bucket and key path")
    bucket, key = rest.split("/", 1)
    return bucket, key


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, bool]:
    event_policy = event.get("catalog_update")
    if isinstance(event_policy, str) and event_policy.strip():
        policy = event_policy.strip()
    else:
        policy = str(os.environ.get("CATALOG_UPDATE_DEFAULT", "on_schema_change")).strip()
    glue_args = event.get("glue_args") or {}
    fp_uri = str(glue_args.get("--schema_fingerprint_s3_uri", "")).strip()

    if policy == "never":
        return {"shouldRunCrawler": False}
    if policy == "force":
        return {"shouldRunCrawler": True}

    # on_schema_change
    if not fp_uri:
        # No fingerprint path; conservatively do not run
        return {"shouldRunCrawler": False}

    bucket, key = _parse_s3_uri(fp_uri)
    s3 = boto3.client("s3")

    # Load latest
    try:
        latest_obj = s3.get_object(Bucket=bucket, Key=key)
        latest = json.loads(latest_obj["Body"].read().decode("utf-8"))
        latest_hash = str(latest.get("hash", ""))
    except Exception:
        # No latest found; treat as changed to initialize catalog
        return {"shouldRunCrawler": True}

    # Load previous
    prev_key = key.rsplit("/", 1)[0] + "/previous.json"
    try:
        prev_obj = s3.get_object(Bucket=bucket, Key=prev_key)
        prev = json.loads(prev_obj["Body"].read().decode("utf-8"))
        prev_hash = str(prev.get("hash", ""))
    except Exception:
        # No previous; consider changed (first time or missing)
        return {"shouldRunCrawler": True}

    return {"shouldRunCrawler": latest_hash != prev_hash}
