"""Lambda stub modules for LocalStack-backed e2e tests."""

from __future__ import annotations

from pathlib import Path


def stub_path(name: str) -> Path:
    """Return the absolute path to the stub module with the given name."""
    return Path(__file__).with_suffix("").parent / f"{name}.py"
