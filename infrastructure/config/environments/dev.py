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
    "tags": {
        "Environment": "dev",
        "Project": "ServerlessDataPipeline",
        "Owner": "DataTeam",
        "CostCenter": "Engineering"
    }
}