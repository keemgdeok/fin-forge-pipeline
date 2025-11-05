"""Typed configuration contracts for environment-specific settings."""

from __future__ import annotations

from typing import Dict, List, NotRequired, Required, TypedDict


class ProcessingTriggerConfig(TypedDict, total=False):
    """Configuration for manifest-driven processing triggers."""

    domain: str
    table_name: str
    file_type: str
    suffixes: List[str]


class LoadDomainConfig(TypedDict, total=False):
    """Configuration for load pipeline domain routing."""

    domain: str
    s3_prefix: str
    priority: str
    layer: NotRequired[str]


class EnvironmentConfig(TypedDict, total=False):
    """Strongly-typed environment configuration contract."""

    region: Required[str]
    account_id: NotRequired[str | None]

    github_oidc_provider_create: NotRequired[bool]
    github_oidc_provider_arn: NotRequired[str]

    lambda_memory: NotRequired[int]
    lambda_timeout: NotRequired[int]
    lambda_additional_s3_patterns: NotRequired[List[str]]

    glue_max_capacity: NotRequired[int]
    glue_max_concurrent_runs: NotRequired[int]
    glue_retry_interval_seconds: NotRequired[int]
    glue_retry_backoff_rate: NotRequired[float]
    glue_retry_max_attempts: NotRequired[int]
    glue_additional_s3_patterns: NotRequired[List[str]]

    step_function_timeout_hours: NotRequired[int]
    sfn_max_concurrency: NotRequired[int]

    s3_retention_days: NotRequired[int]
    log_retention_days: NotRequired[int]
    auto_delete_objects: NotRequired[bool]
    removal_policy: NotRequired[str]

    backup_retention_days: NotRequired[int]
    enable_point_in_time_recovery: NotRequired[bool]

    enable_xray_tracing: NotRequired[bool]
    enable_detailed_monitoring: NotRequired[bool]

    ingestion_symbols: NotRequired[List[str]]
    ingestion_period: NotRequired[str]
    ingestion_interval: NotRequired[str]
    ingestion_file_format: NotRequired[str]
    ingestion_trigger_type: NotRequired[str]
    ingestion_domain: NotRequired[str]
    ingestion_table_name: NotRequired[str]
    ingestion_data_source: NotRequired[str]

    symbol_universe_asset_path: NotRequired[str]
    symbol_universe_asset_file: NotRequired[str]
    symbol_universe_s3_key: NotRequired[str]
    symbol_universe_s3_bucket: NotRequired[str]
    symbol_universe_ssm_param: NotRequired[str]

    orchestrator_chunk_size: NotRequired[int]
    orchestrator_memory: NotRequired[int]
    orchestrator_timeout: NotRequired[int]
    sqs_send_batch_size: NotRequired[int]
    sqs_batch_size: NotRequired[int]

    worker_timeout: NotRequired[int]
    worker_reserved_concurrency: NotRequired[int]
    worker_memory: NotRequired[int]

    enable_gzip: NotRequired[bool]
    raw_manifest_basename: NotRequired[str]
    raw_manifest_suffix: NotRequired[str]

    batch_tracker_table_name: NotRequired[str]
    batch_tracker_ttl_days: NotRequired[int]

    compaction_worker_type: NotRequired[str]
    compaction_number_workers: NotRequired[int]
    compaction_timeout_minutes: NotRequired[int]
    compaction_target_file_mb: NotRequired[int]
    compaction_codec: NotRequired[str]
    compaction_output_subdir: NotRequired[str]
    curated_layer_name: NotRequired[str]

    monitored_glue_jobs: NotRequired[List[str]]
    step_functions_lambda_functions: NotRequired[List[str]]
    step_functions_glue_jobs: NotRequired[List[str]]
    step_functions_glue_crawlers: NotRequired[List[str]]
    monitored_state_machines: NotRequired[List[str]]

    max_retries: NotRequired[int]
    processing_orchestration_mode: NotRequired[str]
    catalog_update: NotRequired[str]

    processing_triggers: NotRequired[List[ProcessingTriggerConfig]]
    processing_suffixes: NotRequired[List[str]]

    load_min_file_size_bytes: NotRequired[int]
    load_domain_configs: NotRequired[List[LoadDomainConfig]]
    allowed_load_layers: NotRequired[List[str]]

    notification_source_email: NotRequired[str]

    tags: NotRequired[Dict[str, str]]
