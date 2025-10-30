"""Glue jobs package initializer.

Keep this lightweight to avoid importing heavy Glue job modules during tests.
Expose placeholders so tests can patch `src.glue.jobs.daily_prices_data_etl`
without importing the real module.
"""

# Placeholder attribute allowing tests to patch without importing the job
daily_prices_data_etl = None
