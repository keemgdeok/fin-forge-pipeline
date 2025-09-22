"""Production environment configuration."""

import os

prod_config = {
    "account_id": os.environ.get("CDK_DEFAULT_ACCOUNT"),
    "region": "ap-northeast-2",
    "lambda_memory": 512,
    "lambda_timeout": 900,
    "glue_max_capacity": 2,
    "step_function_timeout_hours": 4,
    "s3_retention_days": 365,
    "log_retention_days": 90,
    "enable_xray_tracing": True,
    "enable_detailed_monitoring": True,
    "auto_delete_objects": False,
    "removal_policy": "retain",
    "backup_retention_days": 30,
    "enable_point_in_time_recovery": True,
    # Ingestion defaults (conservative by default; adjust to production needs)
    "ingestion_symbols": ["AAPL", "MSFT", "GOOG", "AMZN"],
    "ingestion_period": "6mo",
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
    "orchestrator_chunk_size": 20,
    "sqs_send_batch_size": 10,
    "sqs_batch_size": 2,
    "worker_reserved_concurrency": 15,
    "worker_timeout": 900,
    "worker_memory": 1024,
    "enable_gzip": True,
    "max_retries": 6,
    # Enable full processing orchestration in production so Step Functions + Glue
    # assets are synthesized and deployable by default.
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
        "Environment": "prod",
        "Project": "ServerlessDataPipeline",
        "Owner": "DataTeam",
        "CostCenter": "Production",
    },
}
