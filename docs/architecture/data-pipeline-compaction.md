# Daily Prices Pipeline – Compaction Stage

## Flow Overview
- Raw manifests land in the raw bucket under `interval=.../data_source=.../year=/month=/day=`.
- Step Functions now orchestrates `Preflight → Glue Compaction → Glue Transform → Indicators → Crawler`.
- The compaction Glue job aggregates all raw JSON/CSV files for the partition and materialises one Parquet dataset under `curated/<domain>/<table>/<compacted_subdir>/ds=<ds>/`.
- A lightweight Compaction Guard Lambda confirms that Parquet objects exist; if none are present, the state machine short-circuits the downstream Transform and Indicators jobs.
- Transform Glue job reads the compacted Parquet first (falling back to raw only for legacy partitions) and writes curated data as before.

## Runtime Configuration
- `compaction_output_subdir`, `compaction_codec`, worker type, timeout, and file size targets are defined per-environment in `infrastructure/config/environments/*`.
- Preflight Lambda injects `--compacted_bucket` and `--compacted_prefix` Glue arguments so both compaction and transform jobs remain domain-aware.
- Step Functions map/backfill branches reuse the same compaction parameters and skip logic.

## Monitoring & Alerting
- `monitored_glue_jobs` drives CloudWatch alarms for compaction, transform, and indicators jobs (failure threshold ≥ 1 within 5 minutes) to the shared SNS topic.
- `monitored_state_machines` creates per-state-machine failure alarms alongside the global Step Functions failure alarm.
- Compaction guard logs include the S3 URI that was inspected for quick debugging.

## Operations Guide
- **Reprocessing a day:** re-run the state machine with the same ds; compaction overwrites the Parquet partition deterministically.
- **Compaction failure:** investigate the Glue job run in AWS Console; alarms fire via the shared SNS topic. Re-run after addressing IAM or data quality issues.
- **Empty partitions:** Compaction guard will skip transforms and mark execution as success. No DynamoDB status granularity changes were introduced, per requirements.
- **Hotfix testing:** run `pytest tests/unit/transform/logic/test_raw_to_parquet_compaction.py` locally to validate compaction behaviour before deployment.

## Diagram Update Notes
- Update the architecture diagram to insert a “Compaction (Glue)” node between Preflight and Transform.
- Annotate the Step Functions branch that bypasses Transform when the guard returns `shouldProcess = false`.
