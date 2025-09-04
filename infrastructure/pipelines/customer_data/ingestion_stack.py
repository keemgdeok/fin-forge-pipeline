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
from aws_cdk.aws_lambda_python_alpha import PythonFunction
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

        # Common layer for shared code
        self.common_layer = self._create_common_layer()

        # Customer data ingestion Lambda (real handler)
        self.ingestion_function = self._create_ingestion_function()

        # Event-driven ingestion trigger
        self.ingestion_schedule = self._create_ingestion_schedule()

        self._create_outputs()

    def _create_ingestion_function(self) -> lambda_.IFunction:
        """Create customer data ingestion Lambda function."""
        function = PythonFunction(
            self,
            "CustomerIngestionFunction",
            function_name=f"{self.env_name}-customer-data-ingestion",
            runtime=lambda_.Runtime.PYTHON_3_12,
            entry="src/lambda/functions/data_ingestion",
            index="handler.py",
            handler="main",
            memory_size=self._lambda_memory(),
            timeout=self._lambda_timeout(),
            log_retention=self._log_retention(),
            role=iam.Role.from_role_arn(self, "IngestionLambdaRole", self.lambda_execution_role_arn),
            layers=[self.common_layer],
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

        # Provide a default event payload so the ingestion can actually fetch data
        # Values are sourced from environment config with safe fallbacks
        default_event = self._default_ingestion_event()
        rule.add_target(
            targets.LambdaFunction(
                self.ingestion_function,
                event=events.RuleTargetInput.from_object(default_event),
            )
        )
        return rule

    def _create_outputs(self) -> None:
        """Create CloudFormation outputs."""
        CfnOutput(
            self,
            "IngestionFunctionArn",
            value=self.ingestion_function.function_arn,
            description="Customer data ingestion function ARN",
        )

    def _create_common_layer(self) -> lambda_.LayerVersion:
        """Create Common Layer for shared models and utilities.

        Uses standard Python layer layout: python/shared/... at the root of asset.
        """
        return lambda_.LayerVersion(
            self,
            "CommonLayer",
            layer_version_name=f"{self.env_name}-common-layer",
            description="Shared common models and utilities",
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            code=lambda_.Code.from_asset("src/lambda/layers/common"),
        )

    def _default_ingestion_event(self) -> dict:
        """Build a default ingestion event from config with safe fallbacks."""
        symbols = self.config.get("ingestion_symbols", ["AAPL", "MSFT"])
        period = self.config.get("ingestion_period", "1mo")
        interval = self.config.get("ingestion_interval", "1d")
        file_format = self.config.get("ingestion_file_format", "json")
        # Domain/table reflect current demo scope; adjust per domain pipeline
        domain = self.config.get("ingestion_domain", "market")
        table_name = self.config.get("ingestion_table_name", "prices")

        return {
            "data_source": "yahoo_finance",
            "data_type": "prices",
            "domain": domain,
            "table_name": table_name,
            "symbols": symbols,
            "period": period,
            "interval": interval,
            "file_format": file_format,
        }
