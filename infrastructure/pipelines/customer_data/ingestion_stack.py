"""Customer data ingestion pipeline stack."""

from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_logs as logs,
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
        lambda_execution_role_arn: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.env_name = environment
        self.config = config
        self.shared_storage = shared_storage_stack
        self.lambda_execution_role_arn = lambda_execution_role_arn

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
            function_name=f"{self.env_name}-customer-data-ingestion",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_inline(
                "def lambda_handler(event, context): return {'statusCode': 200}"
            ),  # Placeholder until Phase 2
            memory_size=self._lambda_memory(),
            timeout=self._lambda_timeout(),
            log_retention=self._log_retention(),
            role=iam.Role.from_role_arn(self, "IngestionLambdaRole", self.lambda_execution_role_arn),
            environment={
                "ENVIRONMENT": self.env_name,
                "RAW_BUCKET": self.shared_storage.raw_bucket.bucket_name,
                # "PIPELINE_STATE_TABLE":
                # self.shared_storage.pipeline_state_table.table_name,  # Phase 2
            },
        )

        # S3 권한은 SecurityStack의 LambdaExecutionRole에 최소권한으로 부여됨.
        # (교차 스택 grant로 인한 순환 참조를 피하기 위해 여기서는 grant를 사용하지 않음)

        return function

    def _log_retention(self) -> logs.RetentionDays:
        """Map integer days from config to CloudWatch Logs retention enum."""
        retention_map = {
            1: logs.RetentionDays.ONE_DAY,
            3: logs.RetentionDays.THREE_DAYS,
            5: logs.RetentionDays.FIVE_DAYS,
            7: logs.RetentionDays.ONE_WEEK,
            14: logs.RetentionDays.TWO_WEEKS,
            30: logs.RetentionDays.ONE_MONTH,
            90: logs.RetentionDays.THREE_MONTHS,
        }
        return retention_map.get(self.config.get("log_retention_days", 14), logs.RetentionDays.TWO_WEEKS)

    def _lambda_memory(self) -> int:
        """Resolve Lambda memory size from config (MB)."""
        return int(self.config.get("lambda_memory", 512))

    def _lambda_timeout(self) -> Duration:
        """Resolve Lambda timeout from config (seconds)."""
        return Duration.seconds(int(self.config.get("lambda_timeout", 300)))

    def _create_ingestion_schedule(self) -> events.Rule:
        """Create scheduled trigger for customer data ingestion."""
        rule = events.Rule(
            self,
            "CustomerIngestionSchedule",
            rule_name=f"{self.env_name}-customer-ingestion-schedule",
            schedule=events.Schedule.cron(
                minute="0",
                hour="22",  # Daily at 7 AM (KST)
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
