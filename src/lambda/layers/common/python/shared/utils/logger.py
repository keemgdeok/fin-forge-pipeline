"""Lightweight JSON logger utility for Lambdas.

Provides a consistent, minimal-alloc logger adapter that emits structured
logs with environment and correlation_id fields when available.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        payload: Dict[str, Any] = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        env = getattr(record, "environment", None) or os.environ.get("ENVIRONMENT")
        if env:
            payload["environment"] = env
        corr = getattr(record, "correlation_id", None)
        if corr:
            payload["correlation_id"] = corr
        if not hasattr(record, "asctime"):
            payload["timestamp"] = record.created
        return json.dumps(payload, ensure_ascii=False)


class _Adapter(logging.LoggerAdapter):
    def process(self, msg: Any, kwargs: Dict[str, Any]):  # type: ignore[override]
        extra = self.extra.copy() if isinstance(self.extra, dict) else {}
        if "extra" in kwargs and isinstance(kwargs["extra"], dict):
            extra.update(kwargs["extra"])  # merge per-call extras
        kwargs["extra"] = extra
        return msg, kwargs


def get_logger(name: str, correlation_id: Optional[str] = None) -> logging.LoggerAdapter:
    """Return a JSON-formatted logger adapter with optional correlation_id."""
    base = logging.getLogger(name)
    if not base.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(_JsonFormatter())
        base.addHandler(handler)
    base.setLevel(logging.INFO)
    extras = {"environment": os.environ.get("ENVIRONMENT")}
    if correlation_id:
        extras["correlation_id"] = correlation_id
    return _Adapter(base, extras)


def extract_correlation_id(event: Optional[Dict[str, Any]]) -> Optional[str]:
    """Try to extract a correlation id from common event shapes."""
    if not isinstance(event, dict):
        return None
    for key in ("correlation_id", "CorrelationId", "request_id"):
        val = event.get(key)
        if isinstance(val, str) and val:
            return val
    hdr_obj = event.get("headers")
    headers: Dict[str, Any] = hdr_obj if isinstance(hdr_obj, dict) else {}
    for h in ("x-correlation-id", "x-request-id", "x-amzn-trace-id"):
        hv = headers.get(h)
        if isinstance(hv, str) and hv:
            return hv
    return None
