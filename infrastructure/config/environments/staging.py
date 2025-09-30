"""Staging environment configuration."""

import os

staging_config = {
    "account_id": os.environ.get("CDK_DEFAULT_ACCOUNT"),
    "region": "ap-northeast-2",
    "lambda_memory": 512,
    "lambda_timeout": 600,
    "glue_max_capacity": 2,
    "glue_max_concurrent_runs": 5,
    "step_function_timeout_hours": 4,
    "s3_retention_days": 90,
    "log_retention_days": 30,
    "enable_xray_tracing": True,
    "enable_detailed_monitoring": True,
    "auto_delete_objects": False,
    "removal_policy": "retain",
    # Ingestion defaults
    "ingestion_symbols": ["AAPL", "MSFT", "GOOG"],
    "ingestion_period": "3mo",
    "ingestion_interval": "1d",
    "ingestion_file_format": "json",
    "ingestion_trigger_type": "schedule",
    "ingestion_domain": "market",
    "ingestion_table_name": "prices",
    # Symbol universe asset (deployed via CDK into artifacts bucket)
    "symbol_universe_asset_path": "data/symbols",
    "symbol_universe_asset_file": "all_equities.json",
    "symbol_universe_s3_key": "market/universe/all_equities.json",
    "symbol_universe_s3_bucket": "",
    # Indicators
    "indicators_table_name": "indicators",
    "indicators_lookback_days": 150,
    "indicators_layer": "technical_indicator",
    # Fan-out (Extract) defaults
    "orchestrator_chunk_size": 15,
    "sqs_send_batch_size": 10,
    "sqs_batch_size": 2,
    "worker_reserved_concurrency": 10,
    "worker_timeout": 600,
    "worker_memory": 512,
    "enable_gzip": True,
    "raw_manifest_basename": "_batch",
    "raw_manifest_suffix": ".manifest.json",
    "batch_tracker_table_name": "",
    "batch_tracker_ttl_days": 7,
    "compaction_worker_type": "G.1X",
    "compaction_number_workers": 3,
    "compaction_timeout_minutes": 20,
    "compaction_target_file_mb": 256,
    "compaction_codec": "zstd",
    "compaction_output_subdir": "compacted",
    "glue_retry_interval_seconds": 30,
    "glue_retry_backoff_rate": 2.0,
    "glue_retry_max_attempts": 5,
    "monitored_glue_jobs": [
        "daily-prices-compaction",
        "daily-prices-data-etl",
        "market-indicators-etl",
    ],
    "sfn_max_concurrency": 1,
    "monitored_state_machines": [
        "daily-prices-data-processing",
    ],
    "max_retries": 5,
    "enable_processing_orchestration": False,
    # Catalog update policy for crawler: on_schema_change|never|force
    "catalog_update": "on_schema_change",
    "processing_triggers": [
        {
            "domain": "market",
            "table_name": "prices",
            "file_type": "json",
            "suffixes": [".manifest.json"],
        },
    ],
    "processing_suffixes": [".manifest.json"],
    "load_min_file_size_bytes": 1024,
    "load_domain_configs": [
        {
            "domain": "market",
            "s3_prefix": "market/",
            "priority": "1",
        },
    ],
    "tags": {
        "Environment": "staging",
        "Project": "ServerlessDataPipeline",
        "Owner": "DataTeam",
        "CostCenter": "Engineering",
    },
}
