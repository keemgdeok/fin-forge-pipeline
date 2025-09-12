"""Glue package initializer.

Expose subpackages for import/patch resolution without importing heavy modules.
"""

from . import jobs as jobs

__all__ = ["jobs"]
