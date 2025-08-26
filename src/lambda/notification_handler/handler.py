"""Notification handler Lambda function for pipeline status notifications."""
import json
import logging
import os
from typing import Dict, Any, Optional, List
import boto3
from datetime import datetime
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class NotificationManager:
    """Helper class for managing pipeline notifications."""
    
    def __init__(self, region_name: Optional[str] = None):
        self.sns_client = boto3.client('sns', region_name=region_name)
        self.ses_client = boto3.client('ses', region_name=region_name)
    
    def format_pipeline_message(self, notification_data: Dict[str, Any]) -> Dict[str, str]:
        """Format pipeline notification message."""
        pipeline_name = notification_data.get('pipeline_name', 'Unknown Pipeline')
        status = notification_data.get('status', 'UNKNOWN')
        domain = notification_data.get('domain', 'Unknown')
        table_name = notification_data.get('table_name', 'Unknown')
        environment = notification_data.get('environment', 'Unknown')
        
        # Create detailed message
        message_parts = [
            f"Pipeline: {pipeline_name}",
            f"Domain: {domain}",
            f"Table: {table_name}",
            f"Status: {status}",
            f"Environment: {environment}",
            f"Timestamp: {notification_data.get('timestamp', datetime.utcnow().isoformat())}"
        ]
        
        # Add execution details if available
        if notification_data.get('execution_arn'):
            message_parts.append(f"Execution ARN: {notification_data['execution_arn']}")
        
        if notification_data.get('duration'):
            message_parts.append(f"Duration: {notification_data['duration']}")
        
        # Add error details if status is failed
        if status in ['FAILED', 'ERROR', 'TIMEOUT'] and notification_data.get('error_details'):
            message_parts.extend([
                "",
                "Error Details:",
                f"  Error Type: {notification_data['error_details'].get('error_type', 'Unknown')}",
                f"  Error Message: {notification_data['error_details'].get('error_message', 'No details available')}"
            ])
        
        # Add success details if status is succeeded
        if status == 'SUCCEEDED' and notification_data.get('success_details'):
            success_details = notification_data['success_details']
            message_parts.extend([
                "",
                "Success Details:",
                f"  Records Processed: {success_details.get('records_processed', 'N/A')}",
                f"  Output Location: {success_details.get('output_location', 'N/A')}"
            ])
        
        formatted_message = "\\n".join(message_parts)
        
        # Create subject
        status_emoji = {
            'SUCCEEDED': '✅',
            'FAILED': '❌',
            'ERROR': '🚨',
            'TIMEOUT': '⏰',
            'RUNNING': '🔄',
            'STARTED': '🟡'
        }.get(status, '❓')
        
        subject = f"{status_emoji} Pipeline {status} - {domain}/{table_name} ({environment})"
        
        return {
            'message': formatted_message,
            'subject': subject
        }
    
    def publish_sns_notification(self, topic_arn: str, message: str, subject: str, 
                                message_attributes: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """Publish notification to SNS topic."""
        try:
            publish_params = {
                'TopicArn': topic_arn,
                'Message': message,
                'Subject': subject
            }
            
            if message_attributes:
                formatted_attributes = {}
                for key, value in message_attributes.items():
                    formatted_attributes[key] = {
                        'DataType': 'String',
                        'StringValue': str(value)
                    }
                publish_params['MessageAttributes'] = formatted_attributes
            
            response = self.sns_client.publish(**publish_params)
            message_id = response['MessageId']
            
            logger.info(f"Published SNS notification: {message_id}")
            return message_id
            
        except ClientError as e:
            logger.error(f"Failed to publish SNS notification: {str(e)}")
            return None
    
    def send_email_notification(self, source_email: str, destination_emails: List[str], 
                               subject: str, body: str) -> bool:
        """Send email notification using SES (if configured)."""
        try:
            response = self.ses_client.send_email(
                Source=source_email,
                Destination={'ToAddresses': destination_emails},
                Message={
                    'Subject': {'Data': subject},
                    'Body': {'Text': {'Data': body}}
                }
            )
            
            logger.info(f"Sent email notification: {response['MessageId']}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to send email notification: {str(e)}")
            return False


def main(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main handler for notification Lambda function.
    
    Expected event structure:
    {
        "notification_type": "pipeline_status",  # pipeline_status, error_alert
        "pipeline_name": "string",
        "domain": "string",
        "table_name": "string", 
        "status": "string",                      # SUCCEEDED, FAILED, STARTED, RUNNING
        "execution_arn": "string",
        "duration": "string",
        "environment": "string",
        "error_details": {                       # Only if status is FAILED/ERROR
            "error_type": "string",
            "error_message": "string"
        },
        "success_details": {                     # Only if status is SUCCEEDED
            "records_processed": "number",
            "output_location": "string"
        }
    }
    
    Args:
        event: Lambda event data containing notification details
        context: Lambda context
        
    Returns:
        Response dictionary with notification results
    """
    try:
        logger.info(f"Received notification event: {json.dumps(event, default=str)}")
        
        # Environment variables
        environment = os.environ.get('ENVIRONMENT')
        notification_topic_arn = os.environ.get('NOTIFICATION_TOPIC_ARN')
        source_email = os.environ.get('SOURCE_EMAIL')
        admin_emails = os.environ.get('ADMIN_EMAILS', '').split(',') if os.environ.get('ADMIN_EMAILS') else []
        
        # Extract notification details
        notification_type = event.get('notification_type', 'pipeline_status')
        pipeline_name = event.get('pipeline_name')
        domain = event.get('domain')
        table_name = event.get('table_name')
        status = event.get('status')
        
        # Validate required fields
        required_fields = ['pipeline_name', 'domain', 'status']
        missing_fields = [field for field in required_fields if not event.get(field)]
        
        if missing_fields:
            raise ValueError(f"Missing required notification fields: {missing_fields}")
        
        # Add environment and timestamp to event data
        event['environment'] = environment
        event['timestamp'] = datetime.utcnow().isoformat()
        
        logger.info(f"Processing {notification_type} notification for {domain}/{table_name}: {status}")
        
        # Initialize notification manager
        notification_manager = NotificationManager()
        
        # Format notification message
        formatted_notification = notification_manager.format_pipeline_message(event)
        
        # Prepare message attributes for filtering
        message_attributes = {
            'notification_type': notification_type,
            'domain': domain,
            'status': status,
            'environment': environment
        }
        
        if table_name:
            message_attributes['table_name'] = table_name
        
        # Send SNS notification
        sns_message_id = None
        if notification_topic_arn:
            sns_message_id = notification_manager.publish_sns_notification(
                notification_topic_arn,
                formatted_notification['message'],
                formatted_notification['subject'],
                message_attributes
            )
        else:
            logger.warning("NOTIFICATION_TOPIC_ARN not configured, skipping SNS notification")
        
        # Send email notification for critical statuses
        email_sent = False
        critical_statuses = ['FAILED', 'ERROR', 'TIMEOUT']
        if (status in critical_statuses and source_email and admin_emails and 
            any(email.strip() for email in admin_emails)):
            
            valid_emails = [email.strip() for email in admin_emails if email.strip()]
            email_sent = notification_manager.send_email_notification(
                source_email,
                valid_emails,
                formatted_notification['subject'],
                formatted_notification['message']
            )
        
        # Prepare success response
        result = {
            "statusCode": 200,
            "body": {
                "message": "Notification sent successfully",
                "notification_type": notification_type,
                "pipeline_name": pipeline_name,
                "domain": domain,
                "table_name": table_name,
                "status": status,
                "environment": environment,
                "sns_message_id": sns_message_id,
                "email_sent": email_sent,
                "processed_at": datetime.utcnow().isoformat()
            }
        }
        
        logger.info(f"Successfully processed notification for {domain}/{table_name}: {status}")
        return result
        
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        return {
            "statusCode": 400,
            "body": {
                "error": str(e),
                "message": "Invalid notification event structure"
            }
        }
    
    except Exception as e:
        logger.error(f"Error processing notification: {str(e)}")
        return {
            "statusCode": 500,
            "body": {
                "error": str(e),
                "message": "Failed to process notification"
            }
        }