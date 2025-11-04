#!/usr/bin/env python3
"""
Pipeline-as-a-Product CDK App
Serverless Data Platform following best practices for modular, scalable architecture.
"""

import aws_cdk as cdk

# Core Infrastructure
from infrastructure.core.security_stack import SecurityStack
from infrastructure.core.shared_storage_stack import SharedStorageStack

# Domain Pipelines
from infrastructure.pipelines.daily_prices_data.ingestion_stack import (
    DailyPricesDataIngestionStack,
)
from infrastructure.pipelines.daily_prices_data.processing_stack import (
    DailyPricesDataProcessingStack,
)
from infrastructure.pipelines.load import LoadPipelineStack

# Platform Services
from infrastructure.monitoring.observability_stack import ObservabilityStack
from infrastructure.governance.catalog_stack import DataCatalogStack

# Configuration
from infrastructure.config.environments import get_environment_config

app = cdk.App()

# Get environment configuration
environment = app.node.try_get_context("environment") or "dev"
config = get_environment_config(environment)

# CDK environment (account/region)
cdk_env = cdk.Environment(account=config.get("account_id"), region=config.get("region", "ap-northeast-2"))

stack_prefix = f"DataPlatform-{environment}"

# ========================================
# CORE INFRASTRUCTURE LAYER
# ========================================

# Security Foundation - IAM roles, policies, least privilege
security_stack = SecurityStack(
    app,
    f"{stack_prefix}-Core-Security",
    environment=environment,
    config=config,
    env=cdk_env,
)

# Shared Storage - S3 buckets, DynamoDB tables for platform-wide use
shared_storage_stack = SharedStorageStack(
    app,
    f"{stack_prefix}-Core-SharedStorage",
    environment=environment,
    config=config,
    env=cdk_env,
)

# ========================================
# GOVERNANCE LAYER
# ========================================

# Data Catalog & Governance - Glue Data Catalog, Athena, Lake Formation
catalog_stack = DataCatalogStack(
    app,
    f"{stack_prefix}-Governance-Catalog",
    environment=environment,
    config=config,
    shared_storage_stack=shared_storage_stack,
    env=cdk_env,
)

# ========================================
# DOMAIN-SPECIFIC PIPELINE LAYER
# ========================================

# Daily Prices Data Pipeline
daily_prices_ingestion_stack = DailyPricesDataIngestionStack(
    app,
    f"{stack_prefix}-Pipeline-DailyPricesData-Ingestion",
    environment=environment,
    config=config,
    shared_storage_stack=shared_storage_stack,
    lambda_execution_role_arn=security_stack.lambda_execution_role.role_arn,
    env=cdk_env,
)

daily_prices_processing_stack = DailyPricesDataProcessingStack(
    app,
    f"{stack_prefix}-Pipeline-DailyPricesData-Processing",
    environment=environment,
    config=config,
    shared_storage_stack=shared_storage_stack,
    lambda_execution_role_arn=security_stack.lambda_execution_role.role_arn,
    glue_execution_role_arn=security_stack.glue_execution_role.role_arn,
    step_functions_execution_role_arn=(security_stack.step_functions_execution_role.role_arn),
    batch_tracker_table=daily_prices_ingestion_stack.batch_tracker_table,
    env=cdk_env,
)

load_pipeline_stack = LoadPipelineStack(
    app,
    f"{stack_prefix}-Pipeline-Load",
    environment=environment,
    config=config,
    shared_storage_stack=shared_storage_stack,
    lambda_execution_role_arn=security_stack.lambda_execution_role.role_arn,
    env=cdk_env,
)

# ========================================
# MONITORING & OBSERVABILITY LAYER
# ========================================

# Unified Observability - CloudWatch dashboards, alarms, SNS notifications
observability_stack = ObservabilityStack(
    app,
    f"{stack_prefix}-Monitoring-Observability",
    environment=environment,
    config=config,
    env=cdk_env,
)
observability_stack.add_ingestion_pipeline_monitoring(
    pipeline_name="DailyPricesData",
    queue=daily_prices_ingestion_stack.queue,
    worker_function=daily_prices_ingestion_stack.ingestion_function,
)

# ========================================
# STACK DEPENDENCIES
# ========================================

# Core dependencies - Remove circular dependency by not declaring explicit dependency
catalog_stack.add_dependency(shared_storage_stack)

# Pipeline dependencies - Remove Security dependencies to avoid circular references
daily_prices_ingestion_stack.add_dependency(shared_storage_stack)
# daily_prices_ingestion_stack.add_dependency(security_stack)  # Removed - CDK auto-resolves

daily_prices_processing_stack.add_dependency(shared_storage_stack)
# daily_prices_processing_stack.add_dependency(security_stack)
# Removed - CDK auto-resolves
# daily_prices_processing_stack.add_dependency(daily_prices_ingestion_stack)
# Removed - No direct reference needed

load_pipeline_stack.add_dependency(shared_storage_stack)
observability_stack.add_dependency(daily_prices_ingestion_stack)

# ========================================
# TAGGING STRATEGY
# ========================================

# Platform-wide tags
cdk.Tags.of(app).add("Environment", environment)
cdk.Tags.of(app).add("Platform", "ServerlessDataPipeline")
cdk.Tags.of(app).add("Architecture", "Pipeline-as-a-Product")
cdk.Tags.of(app).add("ManagedBy", "CDK")
cdk.Tags.of(app).add("CostCenter", "DataEngineering")

# Domain-specific tags
cdk.Tags.of(daily_prices_ingestion_stack).add("Domain", "DailyPricesData")
cdk.Tags.of(daily_prices_processing_stack).add("Domain", "DailyPricesData")
cdk.Tags.of(daily_prices_ingestion_stack).add("PipelineType", "Ingestion")
cdk.Tags.of(daily_prices_processing_stack).add("PipelineType", "Processing")
cdk.Tags.of(load_pipeline_stack).add("Domain", "SharedLoad")
cdk.Tags.of(load_pipeline_stack).add("PipelineType", "Load")

app.synth()
