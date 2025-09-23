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
        result = guard.lambda_handler(
            {"bucket": "curated-bucket", "prefix": "market/prices/compacted", "ds": "2024-01-15"},
            None,
        )

    assert result["shouldProcess"] is True
    assert result["objectCount"] == 3
    assert result["partitionPrefix"].endswith("ds=2024-01-15/")


def test_guard_handles_empty_output(monkeypatch):
    guard = _load_guard_module()
    client = MagicMock()
    client.list_objects_v2.return_value = {"KeyCount": 0}

    with patch.object(guard.boto3, "client", return_value=client):
        result = guard.lambda_handler(
            {"bucket": "curated-bucket", "prefix": "market/prices/compacted", "ds": "2024-01-15"},
            None,
        )

    assert result["shouldProcess"] is False
    assert result["objectCount"] == 0


def test_guard_requires_bucket_and_ds(monkeypatch):
    guard = _load_guard_module()
    client = MagicMock()
    client.list_objects_v2.return_value = {"KeyCount": 1}
    with patch.object(guard.boto3, "client", return_value=client):
        with pytest.raises(ValueError):
            guard.lambda_handler({"prefix": "market/prices/compacted", "ds": "2024-01-15"}, None)

    with patch.object(guard.boto3, "client", return_value=client):
        with pytest.raises(ValueError):
            guard.lambda_handler({"bucket": "curated", "prefix": "market/prices/compacted"}, None)


def test_guard_surfaces_s3_errors():
    guard = _load_guard_module()
    client = MagicMock()
    error = ClientError({"Error": {"Code": "AccessDenied", "Message": "Denied"}}, "ListObjectsV2")
    client.list_objects_v2.side_effect = error

    with patch.object(guard.boto3, "client", return_value=client):
        with pytest.raises(RuntimeError, match="Failed to inspect compaction output"):
            guard.lambda_handler(
                {"bucket": "curated-bucket", "prefix": "market/prices/compacted", "ds": "2024-01-15"},
                None,
            )
