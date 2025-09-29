"""Unit tests for the compaction guard Lambda."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError


def _load_guard_module() -> ModuleType:
    module_path = Path("src/lambda/functions/compaction_guard/handler.py")
    spec = importlib.util.spec_from_file_location("compaction_guard_handler", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None  # for mypy/static typing
    spec.loader.exec_module(module)
    return module


def test_guard_detects_compacted_output(monkeypatch):
    guard = _load_guard_module()
    client = MagicMock()
    client.list_objects_v2.return_value = {"KeyCount": 3}

    with patch.object(guard.boto3, "client", return_value=client):
        event = {
            "bucket": "curated-bucket",
            "domain": "market",
            "table_name": "prices",
            "interval": "1d",
            "data_source": "yahoo",
            "layer": "compacted",
            "ds": "2024-01-15",
        }
        result = guard.lambda_handler(event, None)

    assert result["shouldProcess"] is True
    assert result["objectCount"] == 3
    assert result["partitionPrefix"].endswith("layer=compacted")


def test_guard_handles_empty_output(monkeypatch):
    guard = _load_guard_module()
    client = MagicMock()
    client.list_objects_v2.return_value = {"KeyCount": 0}

    with patch.object(guard.boto3, "client", return_value=client):
        event = {
            "bucket": "curated-bucket",
            "domain": "market",
            "table_name": "prices",
            "interval": "1d",
            "data_source": "yahoo",
            "layer": "compacted",
            "ds": "2024-01-15",
        }
        result = guard.lambda_handler(event, None)

    assert result["shouldProcess"] is False
    assert result["objectCount"] == 0


def test_guard_requires_bucket_and_ds(monkeypatch):
    guard = _load_guard_module()
    client = MagicMock()
    client.list_objects_v2.return_value = {"KeyCount": 1}
    base_event = {
        "domain": "market",
        "table_name": "prices",
        "interval": "1d",
        "data_source": "yahoo",
        "layer": "compacted",
        "ds": "2024-01-15",
    }

    with patch.object(guard.boto3, "client", return_value=client):
        with pytest.raises(ValueError):
            guard.lambda_handler(base_event, None)

    with patch.object(guard.boto3, "client", return_value=client):
        with pytest.raises(ValueError):
            event = {**base_event, "bucket": "curated"}
            event.pop("ds")
            guard.lambda_handler(event, None)


def test_guard_surfaces_s3_errors():
    guard = _load_guard_module()
    client = MagicMock()
    error = ClientError({"Error": {"Code": "AccessDenied", "Message": "Denied"}}, "ListObjectsV2")
    client.list_objects_v2.side_effect = error

    with patch.object(guard.boto3, "client", return_value=client):
        with pytest.raises(RuntimeError, match="Failed to inspect compaction output"):
            event = {
                "bucket": "curated-bucket",
                "domain": "market",
                "table_name": "prices",
                "interval": "1d",
                "data_source": "yahoo",
                "layer": "compacted",
                "ds": "2024-01-15",
            }
            guard.lambda_handler(event, None)
