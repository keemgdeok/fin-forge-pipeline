from __future__ import annotations

import re
import os
from typing import Any, Dict


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    """Preflight 단계에서 Glue/Compaction 인자를 구성하는 스텁."""
    manifest_key = event["manifest_key"]
    match = re.search(r"year=(\d{4})/month=(\d{2})/day=(\d{2})", manifest_key)
    if not match:
        raise ValueError(f"Invalid manifest key: {manifest_key}")

    year, month, day = match.groups()
    ds = f"{year}-{month}-{day}"
    domain = event["domain"]
    table = event["table_name"]
    interval = event["interval"]
    data_source = event["data_source"]
    raw_bucket = event["raw_bucket"]
    curated_bucket = event["curated_bucket"]
    artifacts_bucket = event["artifacts_bucket"]
    compacted_layer = event.get("compacted_layer", "compacted")
    curated_layer = event.get("curated_layer", "adjusted")

    base_prefix = f"{domain}/{table}/interval={interval}/data_source={data_source}/year={year}/month={month}/day={day}"

    glue_args = {
        "--raw_bucket": raw_bucket,
        "--raw_prefix": base_prefix,
        "--compacted_bucket": curated_bucket,
        "--curated_bucket": curated_bucket,
        "--domain": domain,
        "--table_name": table,
        "--interval": interval,
        "--data_source": data_source,
        "--ds": ds,
        "--curated_layer": curated_layer,
        "--compacted_layer": compacted_layer,
        "--schema_fingerprint_s3_uri": f"s3://{artifacts_bucket}/{domain}/{table}/_schema/latest.json",
    }
    output_partitions = event.get("output_partitions") or os.environ.get("GLUE_OUTPUT_PARTITIONS") or "4"
    if output_partitions:
        glue_args["--output_partitions"] = str(output_partitions)
    compaction_args = {
        "--raw_bucket": raw_bucket,
        "--raw_prefix": base_prefix,
        "--compacted_bucket": curated_bucket,
        "--layer": compacted_layer,
        "--domain": domain,
        "--table_name": table,
        "--interval": interval,
        "--data_source": data_source,
        "--ds": ds,
    }

    return {
        "proceed": True,
        "manifest_key": manifest_key,
        "ds": ds,
        "glue_args": glue_args,
        "compaction_args": compaction_args,
    }
