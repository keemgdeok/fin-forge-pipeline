"""Glue jobs package initializer.

Keep this lightweight to avoid importing heavy Glue job modules during tests.
Expose placeholders so tests can patch `src.glue.jobs.customer_data_etl`
without importing the real module.
"""

# Placeholder attribute allowing tests to patch without importing the job
customer_data_etl = None
