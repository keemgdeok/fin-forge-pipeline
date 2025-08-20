# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a serverless data pipeline project built on AWS using AWS CDK (Python). It implements a complete data processing workflow using AWS services like Lambda, Step Functions, Glue, S3, Athena, EventBridge, SQS, and SNS.

## Development Commands

```bash
# Environment setup
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
npm install -g aws-cdk

# CDK commands
cdk synth                                    # Synthesize CloudFormation templates
cdk diff                                     # Show differences with deployed stack
cdk deploy --all                             # Deploy all stacks
cdk deploy --context environment=dev        # Deploy to specific environment
cdk destroy --all                           # Destroy all stacks

# Python deployment script
python scripts/deploy/deploy.py --environment dev     # Deploy to dev
python scripts/deploy/deploy.py --environment prod    # Deploy to prod

# Testing
pytest tests/unit/                          # Run unit tests
pytest tests/integration/                   # Run integration tests
pytest tests/ --cov=src/                    # Run tests with coverage

# Code quality
black src/ tests/                           # Format code
flake8 src/ tests/                         # Lint code
mypy src/                                  # Type checking
```

## Architecture

### CDK Stack Structure
- **SecurityStack** (`infrastructure/stacks/security_stack.py`): IAM roles and policies
- **StorageStack** (`infrastructure/stacks/storage_stack.py`): S3 buckets for data lake
- **AnalyticsStack** (`infrastructure/stacks/analytics_stack.py`): Glue Data Catalog and Athena
- **ComputeStack** (`infrastructure/stacks/compute_stack.py`): Lambda functions and Glue jobs
- **OrchestrationStack** (`infrastructure/stacks/orchestration_stack.py`): Step Functions workflows
- **MessagingStack** (`infrastructure/stacks/messaging_stack.py`): EventBridge, SQS, SNS
- **MonitoringStack** (`infrastructure/stacks/monitoring_stack.py`): CloudWatch dashboards and alarms
- **DataPipelineStack** (`infrastructure/stacks/data_pipeline_stack.py`): Main orchestration stack

### Core Components
- **Lambda Functions** (`src/lambda/`): Microservices for data ingestion, ETL triggering, processing, querying, notifications, and error handling
- **Step Functions** (`src/step_functions/`): Orchestrates the data pipeline workflow with error handling
- **Glue Jobs** (`src/glue/jobs/`): ETL processes for raw-to-curated data transformation
- **Shared Layers** (`src/lambda/shared/layers/`): Common utilities and data processing libraries
- **Infrastructure as Code** (`infrastructure/`): CDK Python code for AWS resources

### Data Flow
1. Data ingestion → Raw S3 bucket
2. Glue ETL job triggered via Lambda
3. Data processing and transformation
4. Curated data stored in processed S3 bucket
5. Glue Crawler updates Data Catalog
6. Athena queries available on processed data
7. Notifications sent via SNS

### Environment Management
- Environment-specific configurations in `infrastructure/config/environments/`
- CDK context-based environment switching
- GitHub Actions workflows for automated deployment (`dev`, `staging`, `prod`)

### Key Patterns
- **Stack Dependencies**: Proper dependency management between CDK stacks
- **Constructs**: Reusable components in `infrastructure/constructs/`
- **Environment Configuration**: Type-safe environment configs with validation
- **Lambda Layers**: Shared code across Lambda functions for efficiency
- **IAM Least Privilege**: Minimal required permissions per component
- **Infrastructure as Code**: Everything defined in CDK Python code

### Development Guidelines
- Use Python type hints throughout the codebase
- Follow CDK best practices for stack organization
- Implement proper error handling and logging in Lambda functions
- Use environment-specific configurations for different deployment targets
- Leverage CDK constructs for reusable infrastructure patterns