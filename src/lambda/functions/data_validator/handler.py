"""Data validator Lambda function handler - lightweight orchestrator."""

import json
import logging
import os
from typing import Dict, Any
from datetime import datetime

from shared.validation.data_validator import DataValidator
from shared.validation.validation_rules import StandardValidationRules
from shared.utils.logger import get_logger, extract_correlation_id

logger = get_logger(__name__)


def main(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main handler for data validator Lambda function.

    Expected event structure:
    {
        "source_bucket": "string",
        "source_key": "string",
        "table_name": "string",
        "domain": "string",
        "file_type": "csv|json",
        "validation_rules": {...},          # Optional: override default rules
        "glue_job_config": {...},           # Optional: trigger ETL if validation passes
        "step_function_arn": "string"       # Optional: trigger step function workflow
    }

    Args:
        event: Lambda event data containing validation configuration
        context: Lambda context

    Returns:
        Response dictionary with validation results and ETL trigger status
    """
    try:
        corr_id = extract_correlation_id(event)
        if corr_id:
            globals()["logger"] = get_logger(__name__, correlation_id=corr_id)
        logger.info(f"Received event: {json.dumps(event, default=str)}")

        # Extract event parameters
        source_bucket = event.get("source_bucket")
        source_key = event.get("source_key")
        table_name = event.get("table_name")
        domain = event.get("domain")
        file_type = event.get("file_type", "csv")
        validation_rules = event.get("validation_rules")
        glue_job_config = event.get("glue_job_config", {})
        step_function_arn = event.get("step_function_arn")

        # Environment variables
        environment = os.environ.get("ENVIRONMENT")
        default_source_bucket = os.environ.get("RAW_BUCKET")
        default_target_bucket = os.environ.get("CURATED_BUCKET")

        # Use defaults if not provided
        source_bucket = source_bucket or default_source_bucket

        # Validate required parameters
        if not source_bucket or not source_key:
            raise ValueError("source_bucket and source_key are required")
        if not table_name or not domain:
            raise ValueError("table_name and domain are required")

        logger.info(f"Validating {file_type} file: s3://{source_bucket}/{source_key}")
        logger.info(f"Table: {domain}/{table_name}")

        # Get validation rules (use provided rules or get standard rules by domain)
        if not validation_rules:
            validation_rules = StandardValidationRules.get_validation_rules_by_domain(domain, table_name)
            logger.info(f"Using standard validation rules for domain: {domain}")
        else:
            logger.info("Using custom validation rules from event")

        # Initialize data validator
        validator = DataValidator()

        # Run comprehensive validation
        validation_config = {
            "source_bucket": source_bucket,
            "source_key": source_key,
            "file_type": file_type,
            "validation_rules": validation_rules,
        }

        validation_results = validator.validate_data_comprehensive(validation_config)

        # Check if validation passed
        if not validation_results["overall_valid"]:
            logger.error("Data validation failed")
            return {
                "statusCode": 400,
                "body": {
                    "message": "Data validation failed",
                    "source_location": f"s3://{source_bucket}/{source_key}",
                    "table_name": table_name,
                    "domain": domain,
                    "file_type": file_type,
                    "environment": environment,
                    "validation_results": validation_results,
                    "processed_at": datetime.now().isoformat(),
                },
            }

        logger.info("All data validations passed successfully")

        # Trigger Glue ETL job if configured and validation passed
        etl_job_run_id = None
        if glue_job_config.get("job_name"):
            job_name = glue_job_config["job_name"]
            job_arguments = glue_job_config.get("arguments", {})

            # Add standard arguments
            job_arguments.update(
                {
                    "--environment": environment or "dev",
                    "--source-bucket": source_bucket,
                    "--source-key": source_key,
                    "--target-bucket": default_target_bucket or source_bucket,
                    "--table-name": table_name,
                    "--domain": domain,
                }
            )

            etl_job_run_id = validator.trigger_glue_etl_job(job_name, job_arguments)

            if not etl_job_run_id:
                logger.warning(f"Failed to trigger Glue ETL job: {job_name}")

        # Trigger Step Function workflow if configured
        step_function_execution_arn = None
        if step_function_arn:
            workflow_input = {
                "source_bucket": source_bucket,
                "source_key": source_key,
                "table_name": table_name,
                "domain": domain,
                "file_type": file_type,
                "validation_results": validation_results,
                "etl_job_run_id": etl_job_run_id,
            }

            try:
                response = validator.stepfunctions_client.start_execution(
                    stateMachineArn=step_function_arn,
                    input=json.dumps(workflow_input, default=str),
                    name=f"{domain}-{table_name}-{int(datetime.now().timestamp())}",
                )
                step_function_execution_arn = response["executionArn"]
                logger.info(f"Started Step Function execution: {step_function_execution_arn}")
            except Exception as e:
                logger.warning(f"Failed to start Step Function execution: {str(e)}")

        # Prepare success response
        result = {
            "statusCode": 200,
            "body": {
                "message": "Data validation completed successfully",
                "source_location": f"s3://{source_bucket}/{source_key}",
                "table_name": table_name,
                "domain": domain,
                "file_type": file_type,
                "environment": environment,
                "validation_results": validation_results,
                "etl_job_run_id": etl_job_run_id,
                "step_function_execution_arn": step_function_execution_arn,
                "processed_at": datetime.now().isoformat(),
            },
        }

        logger.info(f"Successfully validated and processed data for {domain}/{table_name}")
        return result

    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        return {
            "statusCode": 400,
            "body": {
                "error": str(e),
                "message": "Invalid request parameters",
                "table_name": event.get("table_name"),
                "domain": event.get("domain"),
                "processed_at": datetime.now().isoformat(),
            },
        }

    except Exception as e:
        logger.error(f"Error in data validation: {str(e)}")
        return {
            "statusCode": 500,
            "body": {
                "error": str(e),
                "message": "Data validation processing failed",
                "table_name": event.get("table_name"),
                "domain": event.get("domain"),
                "processed_at": datetime.now().isoformat(),
            },
        }
