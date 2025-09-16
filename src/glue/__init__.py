"""Glue package initializer.

This module intentionally avoids importing heavy submodules at import time to
keep package initialization lightweight. Consumers should import the required
modules explicitly, e.g. ``from glue import jobs`` or ``import glue.jobs``.
"""

__all__: list[str] = []
