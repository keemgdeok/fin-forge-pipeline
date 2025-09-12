"""Data Quality (DQ) shared utilities.

This package provides a runtime-agnostic evaluation engine for data quality
that can be used from Lambda or Glue. Glue jobs should focus on collecting
metrics (counts, schema checks) and delegate decision logic here.
"""
