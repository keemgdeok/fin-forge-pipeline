from __future__ import annotations

import io
import json
import os
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

from shared.models.events import DataIngestionEvent
from shared.utils.logger import get_logger, extract_correlation_id
from shared.clients.market_data import YahooFinanceClient, PriceRecord


logger = get_logger(__name__)


def _compose_s3_key(
    domain: str,
    table_name: str,
    data_source: str,
    interval: str,
    symbol: str,
    ext: str,
    partition_day: date,
) -> str:
    return (
        f"{domain}/{table_name}/interval={interval}/"
        f"data_source={data_source}/year={partition_day.year:04d}/"
        f"month={partition_day.month:02d}/day={partition_day.day:02d}/"
        f"{symbol}.{ext}"
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


def _content_type(ext: str) -> str:
    if ext == "csv":
        return "text/csv"
    if ext == "json":
        return "application/json"
    return "application/octet-stream"


def process_event(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Core ingestion logic shared by API Lambda and SQS worker."""
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
        manifest_objects: Dict[date, Dict[str, Any]] = defaultdict(lambda: {"objects": [], "raw_prefix": ""})

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

            # Group records by (symbol, UTC 날짜)로 묶어 일자별 파일 생성
            grouped: Dict[Tuple[str, date], List[PriceRecord]] = {}
            for record in fetched:
                ts = record.timestamp
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                else:
                    ts = ts.astimezone(timezone.utc)
                day_key = ts.date()
                grouped.setdefault((record.symbol, day_key), []).append(record)

            for (symbol, day_key), rows in grouped.items():
                body, ext = _serialize_records(rows, file_format)
                key = _compose_s3_key(
                    domain=domain,
                    table_name=table_name,
                    data_source=data_source,
                    interval=interval,
                    symbol=symbol,
                    ext=ext,
                    partition_day=day_key,
                )

                # Idempotency: 동일 키 존재 시 스킵
                try:
                    s3.head_object(Bucket=raw_bucket, Key=key)
                    log.info(
                        "Skip write due to existing object",
                        extra={"bucket": raw_bucket, "key": key},
                    )
                    continue
                except ClientError as exc:
                    error_code = exc.response.get("Error", {}).get("Code")
                    if error_code not in {"404", "NoSuchKey", "NotFound"}:
                        log.exception("Failed to check existing object")
                        raise

                # Optional gzip compression
                enable_gzip = str(os.environ.get("ENABLE_GZIP", "false")).lower() == "true"
                content_type = _content_type(ext)
                content_encoding: Optional[str] = None
                if enable_gzip:
                    import gzip

                    body = gzip.compress(body)
                    key = f"{key}.gz"
                    content_encoding = "gzip"

                put_kwargs: Dict[str, Any] = {
                    "Bucket": raw_bucket,
                    "Key": key,
                    "Body": body,
                    "ContentType": content_type,
                }
                if content_encoding:
                    put_kwargs["ContentEncoding"] = content_encoding

                s3.put_object(**put_kwargs)
                written_keys.append(key)
                manifest_summary = manifest_objects[day_key]
                manifest_summary["objects"].append(
                    {
                        "symbol": symbol,
                        "key": key,
                        "records": len(rows),
                    }
                )
                manifest_summary["raw_prefix"] = (
                    f"{domain}/{table_name}/"
                    f"interval={interval}/"
                    f"data_source={data_source}/"
                    f"year={day_key.year:04d}/month={day_key.month:02d}/day={day_key.day:02d}/"
                )

        # Response
        partition_summaries = [
            {
                "ds": day_key.isoformat(),
                "objects": summary["objects"],
                "raw_prefix": summary["raw_prefix"],
            }
            for day_key, summary in manifest_objects.items()
            if summary["objects"]
        ]
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
                "partition_summaries": partition_summaries,
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
            "body": {
                "error": "UnhandledError",
                "message": "Data ingestion failed",
            },
        }
