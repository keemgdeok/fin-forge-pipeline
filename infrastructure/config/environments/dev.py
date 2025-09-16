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
    "ingestion_domain": "market",
    "ingestion_table_name": "prices",
    # Indicators
    "indicators_table_name": "indicators",
    "indicators_lookback_days": 252,
    # Fan-out (Extract) defaults
    "orchestrator_chunk_size": 10,
    "sqs_send_batch_size": 10,
    "sqs_batch_size": 1,
    "worker_reserved_concurrency": 5,
    "worker_timeout": 300,
    "worker_memory": 512,
    "enable_gzip": False,
    "max_retries": 5,
    "enable_processing_orchestration": True,
    # Catalog update policy for crawler: on_schema_change|never|force
    "catalog_update": "on_schema_change",
    # Processing triggers (S3->EventBridge->SFN). Supports multiple domain/table pairs.
    # Each item: {"domain": str, "table_name": str, "file_type": "json|csv|parquet", "suffixes": [".json", ".csv"]}
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
        {
            "domain": "sales",
            "table_name": "orders",
            "file_type": "json",
            "suffixes": [".json", ".csv"],
        },
    ],
    # Default suffixes when processing_triggers not specified
    "processing_suffixes": [".json", ".csv"],
    # Load pipeline configuration (S3 → SQS)
    "load_min_file_size_bytes": 1024,
    "load_domain_configs": [
        {
            "domain": "market",
            "s3_prefix": "market/",
            "priority": "1",
        },
        {
            "domain": "customer",
            "s3_prefix": "customer/",
            "priority": "2",
        },
        {
            "domain": "product",
            "s3_prefix": "product/",
            "priority": "3",
        },
        {
            "domain": "analytics",
            "s3_prefix": "analytics/",
            "priority": "3",
        },
    ],
    "tags": {
        "Environment": "dev",
        "Project": "ServerlessDataPipeline",
        "Owner": "DataTeam",
        "CostCenter": "Engineering",
    },
}
