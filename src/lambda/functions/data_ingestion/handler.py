"""Data ingestion Lambda function handler - lightweight orchestrator."""

from __future__ import annotations

import io
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3

from shared.models.events import DataIngestionEvent
from shared.utils.logger import get_logger, extract_correlation_id
from shared.clients.market_data import YahooFinanceClient, PriceRecord

logger = get_logger(__name__)


def _compose_s3_key(
    domain: str,
    table_name: str,
    data_source: str,
    symbol: str,
    period: str,
    interval: str,
    ext: str,
    ts: Optional[datetime] = None,
) -> str:
    dt = (ts or datetime.now(timezone.utc)).strftime("%Y-%m-%dT%H-%M-%SZ")
    partition = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return (
        f"{domain}/{table_name}/ingestion_date={partition}/"
        f"data_source={data_source}/symbol={symbol}/interval={interval}/period={period}/"
        f"{dt}.{ext}"
    )


def _serialize_records(records: List[PriceRecord], file_format: str) -> Tuple[bytes, str]:
    fmt = file_format.lower()
    if fmt not in {"json", "csv", "parquet"}:
        fmt = "json"

    if fmt == "csv":
        buf = io.StringIO()
        # header
        buf.write("symbol,timestamp,open,high,low,close,volume\n")
        for r in records:
            buf.write(
                f"{r.symbol},{r.timestamp.isoformat()},{_none_to_empty(r.open)},{_none_to_empty(r.high)},"
                f"{_none_to_empty(r.low)},{_none_to_empty(r.close)},{_none_to_empty(r.volume)}\n"
            )
        return buf.getvalue().encode("utf-8"), "csv"

    if fmt == "parquet":
        # Parquet requires heavy deps; fallback to JSON for now
        fmt = "json"

    # JSON lines
    out = "".join(json.dumps(r.as_dict(), ensure_ascii=False) + "\n" for r in records)
    return out.encode("utf-8"), "json"


def _none_to_empty(v: Optional[float]) -> str:
    return "" if v is None else ("%g" % v)


def main(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main handler for data ingestion Lambda function.

    - Parses event with Pydantic model
    - Optionally fetches market data via Yahoo Finance client (if available)
    - Writes results to S3 with partitioned keys (idempotent-ish via prefix)
    - Returns summary including processed record count and written keys
    """
    try:
        # Use invocation-scoped logger to avoid correlation id leakage
        corr_id = extract_correlation_id(event)
        log = get_logger(__name__, correlation_id=corr_id) if corr_id else logger
        log.info(f"Received event: {json.dumps(event, default=str)}")

        # Environment variables
        raw_bucket = os.environ.get("RAW_BUCKET")
        environment = os.environ.get("ENVIRONMENT")

        log.info(f"Processing data ingestion for environment: {environment}")

        # Parse expected inputs with safe defaults using typed model
        model = DataIngestionEvent.model_validate(event)
        data_source = model.data_source
        data_type = model.data_type
        symbols_raw: List[Any] = event.get("symbols", []) if isinstance(event, dict) else []
        valid_symbols = model.symbols
        invalid_symbols = [s for s in symbols_raw if not isinstance(s, str) or not str(s).strip()]
        period = model.period
        interval = model.interval
        domain = model.domain
        table_name = model.table_name
        file_format = model.file_format

        processed_records = 0
        written_keys: List[str] = []

        # Fetch data (currently only yahoo_finance supported; optional dependency)
        fetched: List[PriceRecord] = []
        if data_source == "yahoo_finance" and data_type == "prices" and valid_symbols:
            client = YahooFinanceClient()
            fetched = client.fetch_prices(valid_symbols, period, interval)
            processed_records = len(fetched)
            log.info(
                f"Fetched records: {processed_records} for symbols={valid_symbols} period={period} interval={interval}"
            )
        else:
            log.warning(f"Unsupported data_source/data_type or no symbols: {data_source}/{data_type}")

        # Persist to S3 if we have data and bucket configured
        if fetched and raw_bucket:
            s3 = boto3.client("s3")

            # Group by symbol to keep files small
            by_symbol: Dict[str, List[PriceRecord]] = {}
            for r in fetched:
                by_symbol.setdefault(r.symbol, []).append(r)

            for sym, rows in by_symbol.items():
                body, ext = _serialize_records(rows, file_format)
                key = _compose_s3_key(domain, table_name, data_source, sym, period, interval, ext)

                # Idempotency: if any object exists under the prefix for this date/symbol/period/interval, skip
                prefix = (
                    f"{domain}/{table_name}/ingestion_date="
                    f"{datetime.now(timezone.utc).strftime('%Y-%m-%d')}/data_source={data_source}/"
                    f"symbol={sym}/interval={interval}/period={period}/"
                )

                try:
                    existed = False
                    resp = s3.list_objects_v2(Bucket=raw_bucket, Prefix=prefix, MaxKeys=1)
                    if int(resp.get("KeyCount", 0)) > 0:
                        existed = True
                    if existed:
                        log.info(f"Skip write due to idempotency prefix exists: s3://{raw_bucket}/{prefix}")
                        continue
                except Exception:
                    # Non-fatal; proceed with put
                    pass

                s3.put_object(Bucket=raw_bucket, Key=key, Body=body, ContentType=_content_type(ext))
                written_keys.append(key)

        # Downstream orchestration is decoupled from ingestion Lambda.
        # Any processing workflows should be triggered by S3/EventBridge rules.

        # Response
        result = {
            "statusCode": 200,
            "body": {
                "message": "Data ingestion completed",
                "data_source": data_source,
                "data_type": data_type,
                "symbols_requested": symbols_raw,
                "symbols_processed": valid_symbols,
                "invalid_symbols": invalid_symbols,
                "period": period,
                "interval": interval,
                "domain": domain,
                "table_name": table_name,
                "file_format": file_format,
                "environment": environment,
                "raw_bucket": raw_bucket,
                "processed_records": processed_records,
                "written_keys": written_keys,
            },
        }

        log.info("Data ingestion completed successfully")
        return result

    except Exception:
        # Include stack trace for better observability
        log = logger
        log.exception("Error in data ingestion")
        return {
            "statusCode": 500,
            "body": {"error": "UnhandledError", "message": "Data ingestion failed"},
        }


def _content_type(ext: str) -> str:
    if ext == "csv":
        return "text/csv"
    if ext == "json":
        return "application/json"
    return "application/octet-stream"
