"""Error handler Lambda function for centralized error processing."""

import json
import logging
import os
from typing import Dict, Any, Optional
import boto3
from datetime import datetime
from botocore.exceptions import ClientError

from shared.utils.logger import get_logger, extract_correlation_id

logger = get_logger(__name__)


class ErrorProcessor:
    """Helper class for processing and routing errors."""

    def __init__(self, region_name: Optional[str] = None):
        self.sns_client = boto3.client("sns", region_name=region_name)
        self.cloudwatch_client = boto3.client("cloudwatch", region_name=region_name)

    def publish_error_notification(self, topic_arn: str, error_details: Dict[str, Any]) -> bool:
        """Publish error notification to SNS topic."""
        try:
            message = {
                "timestamp": datetime.utcnow().isoformat(),
                "severity": error_details.get("severity", "ERROR"),
                "source": error_details.get("source", "unknown"),
                "error_type": error_details.get("error_type", "unknown"),
                "error_message": error_details.get("error_message", ""),
                "context": error_details.get("context", {}),
                "environment": error_details.get("environment", "unknown"),
            }

            subject = (
                "Pipeline Error - "
                f"{error_details.get('source', 'Unknown')} - "
                f"{error_details.get('environment', 'Unknown')}"
            )

            response = self.sns_client.publish(
                TopicArn=topic_arn, Message=json.dumps(message, indent=2), Subject=subject
            )

            logger.info(f"Published error notification to SNS: {response['MessageId']}")
            return True

        except ClientError as e:
            logger.error(f"Failed to publish error notification: {str(e)}")
            return False

    def put_custom_metric(
        self,
        metric_name: str,
        value: float,
        unit: str = "Count",
        namespace: str = "DataPipeline/Errors",
        dimensions: Optional[Dict[str, str]] = None,
    ) -> bool:
        """Put custom metric to CloudWatch."""
        try:
            metric_data = {"MetricName": metric_name, "Value": value, "Unit": unit, "Timestamp": datetime.utcnow()}

            if dimensions:
                metric_data["Dimensions"] = [{"Name": key, "Value": value} for key, value in dimensions.items()]

            self.cloudwatch_client.put_metric_data(Namespace=namespace, MetricData=[metric_data])

            logger.info(f"Put custom metric '{metric_name}' to CloudWatch")
            return True

        except ClientError as e:
            logger.error(f"Failed to put custom metric: {str(e)}")
            return False


def main(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main handler for error processing Lambda function.

    Expected event structure:
    {
        "error": {
            "source": "string",           # Source service/function that generated error
            "error_type": "string",       # Type of error (ValidationError, ProcessingError, etc.)
            "error_message": "string",    # Detailed error message
            "severity": "string",         # ERROR, WARNING, CRITICAL
            "context": {                  # Additional context
                "table_name": "string",
                "domain": "string",
                "job_run_id": "string",
                "step_function_execution_arn": "string"
            }
        }
    }

    Args:
        event: Lambda event data containing error details
        context: Lambda context

    Returns:
        Response dictionary with processing results
    """
    try:
        corr_id = extract_correlation_id(event)
        if corr_id:
            globals()["logger"] = get_logger(__name__, correlation_id=corr_id)
        logger.info(f"Received error event: {json.dumps(event, default=str)}")

        # Extract error details from event
        error_details = event.get("error", {})

        # Environment variables
        environment = os.environ.get("ENVIRONMENT")
        error_topic_arn = os.environ.get("ERROR_TOPIC_ARN")

        # Add environment to error context
        error_details["environment"] = environment

        # Validate required fields
        required_fields = ["source", "error_message"]
        missing_fields = [field for field in required_fields if not error_details.get(field)]

        if missing_fields:
            raise ValueError(f"Missing required error fields: {missing_fields}")

        # Set default values
        error_details.setdefault("severity", "ERROR")
        error_details.setdefault("error_type", "UnknownError")
        error_details.setdefault("context", {})

        logger.error(f"Processing error from {error_details['source']}: {error_details['error_message']}")

        # Initialize error processor
        error_processor = ErrorProcessor()

        # Publish error notification if topic ARN is configured
        notification_sent = False
        if error_topic_arn:
            notification_sent = error_processor.publish_error_notification(error_topic_arn, error_details)
        else:
            logger.warning("ERROR_TOPIC_ARN not configured, skipping notification")

        # Put custom metrics
        metric_dimensions = {
            "Environment": environment,
            "Source": error_details["source"],
            "ErrorType": error_details["error_type"],
            "Severity": error_details["severity"],
        }

        # Add domain and table if available in context
        context_info = error_details.get("context", {})
        if context_info.get("domain"):
            metric_dimensions["Domain"] = context_info["domain"]
        if context_info.get("table_name"):
            metric_dimensions["TableName"] = context_info["table_name"]

        # Put error count metric
        error_processor.put_custom_metric("ErrorCount", 1.0, "Count", "DataPipeline/Errors", metric_dimensions)

        # Put severity-specific metric
        error_processor.put_custom_metric(
            f'{error_details["severity"]}Count', 1.0, "Count", "DataPipeline/Errors", metric_dimensions
        )

        # Prepare success response
        result = {
            "statusCode": 200,
            "body": {
                "message": "Error processed successfully",
                "error_source": error_details["source"],
                "error_type": error_details["error_type"],
                "severity": error_details["severity"],
                "notification_sent": notification_sent,
                "environment": environment,
                "processed_at": datetime.utcnow().isoformat(),
            },
        }

        logger.info(f"Successfully processed error from {error_details['source']}")
        return result

    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        return {"statusCode": 400, "body": {"error": str(e), "message": "Invalid error event structure"}}

    except Exception as e:
        logger.error(f"Error processing error event: {str(e)}")
        return {"statusCode": 500, "body": {"error": str(e), "message": "Failed to process error event"}}

