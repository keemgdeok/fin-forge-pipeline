"""Customer data ingestion pipeline stack."""
from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    aws_events as events,
    aws_events_targets as targets,
    aws_s3 as s3,
    Duration,
    CfnOutput,
)
from constructs import Construct


class CustomerDataIngestionStack(Stack):
    """Customer data ingestion pipeline."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: str,
        config: dict,
        shared_storage_stack,
        security_stack,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.environment = environment
        self.config = config
        self.shared_storage = shared_storage_stack
        self.security = security_stack

        # Customer data ingestion Lambda
        self.ingestion_function = self._create_ingestion_function()
        
        # Event-driven ingestion trigger
        self.ingestion_schedule = self._create_ingestion_schedule()

        self._create_outputs()

    def _create_ingestion_function(self) -> lambda_.Function:
        """Create customer data ingestion Lambda function."""
        function = lambda_.Function(
            self,
            "CustomerIngestionFunction",
            function_name=f"{self.environment}-customer-data-ingestion",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("src/lambda/customer_data_ingestion"),
            timeout=Duration.minutes(5),
            role=self.security.lambda_execution_role,
            environment={
                "ENVIRONMENT": self.environment,
                "RAW_BUCKET": self.shared_storage.raw_bucket.bucket_name,
                "PIPELINE_STATE_TABLE": self.shared_storage.pipeline_state_table.table_name,
            },
        )

        # Grant S3 permissions
        self.shared_storage.raw_bucket.grant_write(function)
        self.shared_storage.pipeline_state_table.grant_read_write_data(function)

        return function

    def _create_ingestion_schedule(self) -> events.Rule:
        """Create scheduled trigger for customer data ingestion."""
        rule = events.Rule(
            self,
            "CustomerIngestionSchedule",
            rule_name=f"{self.environment}-customer-ingestion-schedule",
            schedule=events.Schedule.cron(
                minute="0",
                hour="2",  # Daily at 2 AM
            ),
        )

        rule.add_target(targets.LambdaFunction(self.ingestion_function))
        return rule

    def _create_outputs(self) -> None:
        """Create CloudFormation outputs."""
        CfnOutput(
            self,
            "IngestionFunctionArn",
            value=self.ingestion_function.function_arn,
            description="Customer data ingestion function ARN",
        )