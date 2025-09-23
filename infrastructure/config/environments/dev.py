"""Development environment configuration."""

import os

dev_config = {
    "account_id": os.environ.get("CDK_DEFAULT_ACCOUNT"),
    "region": "ap-northeast-2",
    "lambda_memory": 512,
    "lambda_timeout": 300,
    "glue_max_capacity": 2,
    "step_function_timeout_hours": 2,
    "s3_retention_days": 30,
    "log_retention_days": 14,
    "enable_xray_tracing": True,
    "enable_detailed_monitoring": True,
    "auto_delete_objects": True,
    "removal_policy": "destroy",
    # Ingestion defaults (EventBridge -> Lambda input)
    "ingestion_symbols": ["AAPL", "MSFT"],
    "ingestion_period": "1mo",
    "ingestion_interval": "1d",
    "ingestion_file_format": "json",
    "ingestion_trigger_type": "schedule",
    "ingestion_domain": "market",
    "ingestion_table_name": "prices",
    # Symbol universe asset (deployed via CDK into artifacts bucket)
    "symbol_universe_asset_path": "data/symbols",
    "symbol_universe_asset_file": "nasdaq_sp500.json",
    "symbol_universe_s3_key": "market/universe/nasdaq_sp500.json",
    "symbol_universe_s3_bucket": "",
    # Indicators
    "indicators_table_name": "indicators",
    "indicators_lookback_days": 252,
    # Fan-out (Extract) defaults
    "orchestrator_chunk_size": 10,
    "sqs_send_batch_size": 10,
    "sqs_batch_size": 1,
    "worker_timeout": 300,
    "worker_reserved_concurrency": 0,
    "worker_memory": 512,
    "enable_gzip": False,
    "raw_manifest_basename": "_batch",
    "raw_manifest_suffix": ".manifest.json",
    "max_retries": 5,
    "enable_processing_orchestration": True,
    # Catalog update policy for crawler: on_schema_change|never|force
    "catalog_update": "on_schema_change",
    # Processing triggers (S3->EventBridge->SFN). Supports multiple domain/table pairs.
    # Each item: {"domain": str, "table_name": str, "file_type": "json|csv|parquet", "suffixes": [".manifest.json"]}
    "processing_triggers": [
        {
            "domain": "market",
            "table_name": "prices",
            "file_type": "json",
            "suffixes": [".manifest.json"],
        },
        {
            "domain": "market",
            "table_name": "indicators",
            "file_type": "parquet",
            "suffixes": [".parquet"],
        },
    ],
    # Default suffixes when processing_triggers not specified
    "processing_suffixes": [".manifest.json"],
    # Load pipeline configuration (S3 → SQS)
    "load_min_file_size_bytes": 1024,
    "load_domain_configs": [
        {
            "domain": "market",
            "s3_prefix": "market/",
            "priority": "1",
        },
    ],
    "tags": {
        "Environment": "dev",
        "Project": "ServerlessDataPipeline",
        "Owner": "DataTeam",
        "CostCenter": "Engineering",
    },
}
