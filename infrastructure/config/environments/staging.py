"""Staging environment configuration."""
import os

staging_config = {
    "account_id": os.environ.get("CDK_DEFAULT_ACCOUNT"),
    "region": "us-east-1",
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
    "tags": {
        "Environment": "staging",
        "Project": "ServerlessDataPipeline",
        "Owner": "DataTeam",
        "CostCenter": "Engineering"
    }
}