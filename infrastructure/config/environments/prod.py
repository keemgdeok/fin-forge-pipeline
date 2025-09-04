"""Production environment configuration."""

import os

prod_config = {
    "account_id": os.environ.get("CDK_DEFAULT_ACCOUNT"),
    "region": "us-east-1",
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
    "ingestion_domain": "market",
    "ingestion_table_name": "prices",
    "tags": {
        "Environment": "prod",
        "Project": "ServerlessDataPipeline",
        "Owner": "DataTeam",
        "CostCenter": "Production",
    },
}
