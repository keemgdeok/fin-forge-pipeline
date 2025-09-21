"""Staging environment configuration."""

import os

staging_config = {
    "account_id": os.environ.get("CDK_DEFAULT_ACCOUNT"),
    "region": "ap-northeast-2",
    "lambda_memory": 512,
    "lambda_timeout": 600,
    "glue_max_capacity": 2,
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
    "ingestion_domain": "market",
    "ingestion_table_name": "prices",
    # Symbol universe asset (deployed via CDK into artifacts bucket)
    "symbol_universe_asset_path": "data/symbols",
    "symbol_universe_asset_file": "nasdaq_sp500.json",
    "symbol_universe_s3_key": "market/universe/nasdaq_sp500.json",
    "symbol_universe_s3_bucket": None,
    # Indicators
    "indicators_table_name": "indicators",
    "indicators_lookback_days": 252,
    # Fan-out (Extract) defaults
    "orchestrator_chunk_size": 15,
    "sqs_send_batch_size": 10,
    "sqs_batch_size": 2,
    "worker_reserved_concurrency": 10,
    "worker_timeout": 600,
    "worker_memory": 512,
    "enable_gzip": True,
    "max_retries": 5,
    "enable_processing_orchestration": True,
    # Catalog update policy for crawler: on_schema_change|never|force
    "catalog_update": "on_schema_change",
    "processing_triggers": [
        {
            "domain": "market",
            "table_name": "prices",
            "file_type": "json",
            "suffixes": [".json", ".csv"],
        },
        {
            "domain": "market",
            "table_name": "indicators",
            "file_type": "parquet",
            "suffixes": [".parquet"],
        },
    ],
    "processing_suffixes": [".json", ".csv"],
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
