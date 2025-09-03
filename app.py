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
from infrastructure.pipelines.customer_data.ingestion_stack import CustomerDataIngestionStack
from infrastructure.pipelines.customer_data.processing_stack import CustomerDataProcessingStack

# Platform Services
from infrastructure.monitoring.observability_stack import ObservabilityStack
from infrastructure.governance.catalog_stack import DataCatalogStack

# Configuration
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

stack_prefix = f"DataPlatform-{environment}"

# ========================================
# CORE INFRASTRUCTURE LAYER
# ========================================

# Security Foundation - IAM roles, policies, least privilege
security_stack = SecurityStack(
    app, f"{stack_prefix}-Core-Security", **stack_props
)

# Shared Storage - S3 buckets, DynamoDB tables for platform-wide use
shared_storage_stack = SharedStorageStack(
    app, f"{stack_prefix}-Core-SharedStorage", **stack_props
)

# ========================================
# GOVERNANCE LAYER
# ========================================

# Data Catalog & Governance - Glue Data Catalog, Athena, Lake Formation
catalog_stack = DataCatalogStack(
    app, f"{stack_prefix}-Governance-Catalog",
    shared_storage_stack=shared_storage_stack,
    **stack_props
)

# ========================================
# MONITORING & OBSERVABILITY LAYER
# ========================================

# Unified Observability - CloudWatch dashboards, alarms, SNS notifications
observability_stack = ObservabilityStack(
    app, f"{stack_prefix}-Monitoring-Observability", **stack_props
)

# ========================================
# DOMAIN-SPECIFIC PIPELINE LAYER
# ========================================

# Customer Data Pipeline
customer_ingestion_stack = CustomerDataIngestionStack(
    app, f"{stack_prefix}-Pipeline-CustomerData-Ingestion",
    shared_storage_stack=shared_storage_stack,
    security_stack=security_stack,
    **stack_props
)

customer_processing_stack = CustomerDataProcessingStack(
    app, f"{stack_prefix}-Pipeline-CustomerData-Processing",
    shared_storage_stack=shared_storage_stack,
    security_stack=security_stack,
    **stack_props
)

# TODO: Add more domain pipelines
# - ProductAnalyticsIngestionStack
# - ProductAnalyticsProcessingStack
# - OrderProcessingIngestionStack
# - OrderProcessingProcessingStack
# - FinanceReportingIngestionStack
# - FinanceReportingProcessingStack

# ========================================
# STACK DEPENDENCIES
# ========================================

# Core dependencies - Remove circular dependency by not declaring explicit dependency
catalog_stack.add_dependency(shared_storage_stack)

# Pipeline dependencies - Remove Security dependencies to avoid circular references
customer_ingestion_stack.add_dependency(shared_storage_stack)
# customer_ingestion_stack.add_dependency(security_stack)  # Removed - CDK auto-resolves

customer_processing_stack.add_dependency(shared_storage_stack) 
# customer_processing_stack.add_dependency(security_stack)  # Removed - CDK auto-resolves
customer_processing_stack.add_dependency(customer_ingestion_stack)

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
cdk.Tags.of(customer_ingestion_stack).add("Domain", "CustomerData")
cdk.Tags.of(customer_processing_stack).add("Domain", "CustomerData")
cdk.Tags.of(customer_ingestion_stack).add("PipelineType", "Ingestion")
cdk.Tags.of(customer_processing_stack).add("PipelineType", "Processing")

app.synth()