#!/usr/bin/env python3
import aws_cdk as cdk
from infrastructure.stacks.data_pipeline_stack import DataPipelineStack
from infrastructure.stacks.storage_stack import StorageStack
from infrastructure.stacks.compute_stack import ComputeStack
from infrastructure.stacks.orchestration_stack import OrchestrationStack
from infrastructure.stacks.messaging_stack import MessagingStack
from infrastructure.stacks.analytics_stack import AnalyticsStack
from infrastructure.stacks.monitoring_stack import MonitoringStack
from infrastructure.stacks.security_stack import SecurityStack
from infrastructure.config.environments import get_environment_config

app = cdk.App()

# Get environment configuration
environment = app.node.try_get_context("environment") or "dev"
config = get_environment_config(environment)

# Common stack properties
stack_props = {
    "env": cdk.Environment(
        account=config.get("account_id"),
        region=config.get("region", "us-east-1")
    ),
    "environment": environment,
    "config": config,
}

stack_prefix = f"DataPipeline-{environment}"

# Security Stack (IAM roles, policies)
security_stack = SecurityStack(
    app, f"{stack_prefix}-Security", **stack_props
)

# Storage Stack (S3 buckets)
storage_stack = StorageStack(
    app, f"{stack_prefix}-Storage", **stack_props
)

# Analytics Stack (Glue Database, Athena)
analytics_stack = AnalyticsStack(
    app, f"{stack_prefix}-Analytics",
    raw_bucket=storage_stack.raw_bucket,
    curated_bucket=storage_stack.curated_bucket,
    **stack_props
)

# Compute Stack (Lambda functions, Glue jobs)
compute_stack = ComputeStack(
    app, f"{stack_prefix}-Compute",
    storage_stack=storage_stack,
    analytics_stack=analytics_stack,
    security_stack=security_stack,
    **stack_props
)

# Orchestration Stack (Step Functions)
orchestration_stack = OrchestrationStack(
    app, f"{stack_prefix}-Orchestration",
    compute_stack=compute_stack,
    **stack_props
)

# Messaging Stack (EventBridge, SQS, SNS)
messaging_stack = MessagingStack(
    app, f"{stack_prefix}-Messaging",
    orchestration_stack=orchestration_stack,
    **stack_props
)

# Main Data Pipeline Stack (combines all components)
data_pipeline_stack = DataPipelineStack(
    app, f"{stack_prefix}-Pipeline",
    storage_stack=storage_stack,
    compute_stack=compute_stack,
    orchestration_stack=orchestration_stack,
    messaging_stack=messaging_stack,
    analytics_stack=analytics_stack,
    **stack_props
)

# Monitoring Stack (CloudWatch, Alarms, Dashboards)
monitoring_stack = MonitoringStack(
    app, f"{stack_prefix}-Monitoring",
    compute_stack=compute_stack,
    orchestration_stack=orchestration_stack,
    **stack_props
)

# Set up dependencies
analytics_stack.add_dependency(storage_stack)
compute_stack.add_dependency(security_stack)
compute_stack.add_dependency(storage_stack)
compute_stack.add_dependency(analytics_stack)
orchestration_stack.add_dependency(compute_stack)
messaging_stack.add_dependency(orchestration_stack)
data_pipeline_stack.add_dependency(messaging_stack)
monitoring_stack.add_dependency(compute_stack)
monitoring_stack.add_dependency(orchestration_stack)

# Add common tags
cdk.Tags.of(app).add("Environment", environment)
cdk.Tags.of(app).add("Project", "ServerlessDataPipeline")
cdk.Tags.of(app).add("ManagedBy", "CDK")

app.synth()