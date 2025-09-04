"""Development environment configuration."""

import os

dev_config = {
    "account_id": os.environ.get("CDK_DEFAULT_ACCOUNT"),
    "region": "us-east-1",
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
    "tags": {
        "Environment": "dev",
        "Project": "ServerlessDataPipeline",
        "Owner": "DataTeam",
        "CostCenter": "Engineering",
    },
}
