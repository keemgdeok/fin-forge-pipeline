"""Daily prices ingestion pipeline stack."""

from pathlib import Path, PurePosixPath

from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_logs as logs,
    aws_s3 as s3,
    aws_s3_deployment as s3_deployment,
    aws_sqs as sqs,
    aws_dynamodb as dynamodb,
    Duration,
    CfnOutput,
    RemovalPolicy,
)
from aws_cdk import aws_lambda_event_sources as lambda_event_sources
from aws_cdk.aws_lambda_python_alpha import BundlingOptions, PythonFunction, PythonLayerVersion
from constructs import Construct
from aws_cdk import aws_cloudwatch as cw


class DailyPricesDataIngestionStack(Stack):
    """Daily prices ingestion pipeline."""

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
        self.data_source: str = str(self.config.get("ingestion_data_source") or "yahoo_finance")

        # Common layer for shared code
        self.common_layer = self._create_common_layer()

        # Market domain layer for pipeline-specific code
        self.market_domain_layer = self._create_market_domain_layer()

        # Market data dependency layer (e.g., yfinance/pandas)
        self.market_data_dependencies_layer = self._create_market_data_dependencies_layer()

        # Queues for fan-out processing
        self.dlq, self.queue = self._create_queues()

        # DynamoDB table used to track batch completion across worker Lambdas
        self.batch_tracker_table = self._create_batch_tracker_table()

        # Orchestrator Lambda (triggered by schedule)
        self.orchestrator_function = self._create_orchestrator_function()

        # Publish symbol universe asset into artifacts bucket when configured
        self._deploy_symbol_universe_asset()

        # Worker Lambda (triggered by SQS)
        self.ingestion_function = self._create_worker_function()

        # Grant orchestrator and worker access to the batch tracker table
        self.batch_tracker_table.grant_read_write_data(self.orchestrator_function)
        self.batch_tracker_table.grant_read_write_data(self.ingestion_function)

        # Event-driven orchestrator trigger (schedule)
        self.ingestion_schedule = self._create_ingestion_schedule()

        # Alarms and Dashboard for Extract
        self._create_alarms_and_dashboard()

        self._create_outputs()

    def _create_worker_function(self) -> lambda_.IFunction:
        """Create ingestion worker Lambda function subscribed to SQS."""
        memory = int(self.config.get("worker_memory", self._lambda_memory()))
        timeout = Duration.seconds(int(self.config.get("worker_timeout", self.config.get("lambda_timeout", 300))))

        reserved_concurrency = self.config.get("worker_reserved_concurrency")
        if reserved_concurrency is not None:
            reserved_concurrency = int(reserved_concurrency)
            if reserved_concurrency <= 0:
                reserved_concurrency = None

        function = PythonFunction(
            self,
            "DailyPricesIngestionWorker",
            function_name=f"{self.env_name}-daily-prices-data-ingestion-worker",
            runtime=lambda_.Runtime.PYTHON_3_12,
            entry="src/lambda/functions/ingestion_worker",
            index="handler.py",
            handler="main",
            memory_size=memory,
            timeout=timeout,
            log_retention=self._log_retention(),
            role=iam.Role.from_role_arn(self, "IngestionWorkerRole", self.lambda_execution_role_arn),
            layers=[self.common_layer, self.market_domain_layer, self.market_data_dependencies_layer],
            environment={
                "ENVIRONMENT": self.env_name,
                "RAW_BUCKET": self.shared_storage.raw_bucket.bucket_name,
                "ENABLE_GZIP": str(bool(self.config.get("enable_gzip", False))).lower(),
                "RAW_MANIFEST_BASENAME": str(self.config.get("raw_manifest_basename", "_batch")),
                "RAW_MANIFEST_SUFFIX": str(self.config.get("raw_manifest_suffix", ".manifest.json")),
                "BATCH_TRACKING_TABLE": self.batch_tracker_table.table_name,
            },
            reserved_concurrent_executions=reserved_concurrency if reserved_concurrency is not None else None,
        )

        # Subscribe to SQS with batch size from config (default 1)
        batch_size = int(self.config.get("sqs_batch_size", 1))
        function.add_event_source(
            lambda_event_sources.SqsEventSource(
                self.queue,
                batch_size=batch_size,
                report_batch_item_failures=True,
            )
        )
        return function

    def _create_orchestrator_function(self) -> lambda_.IFunction:
        """Create orchestrator Lambda function to fan-out symbols into SQS."""
        env_vars: dict[str, str] = {
            "ENVIRONMENT": self.env_name,
            "QUEUE_URL": self.queue.queue_url,
            "CHUNK_SIZE": str(int(self.config.get("orchestrator_chunk_size", 5))),
            "SQS_SEND_BATCH_SIZE": str(int(self.config.get("sqs_send_batch_size", 10))),
            "BATCH_TRACKING_TABLE": self.batch_tracker_table.table_name,
            "BATCH_TRACKER_TTL_DAYS": str(int(self.config.get("batch_tracker_ttl_days", 7))),
        }

        ssm_param = self.config.get("symbol_universe_ssm_param")
        if ssm_param:
            env_vars["SYMBOLS_SSM_PARAM"] = str(ssm_param)

        symbols_s3_key = self.config.get("symbol_universe_s3_key")
        if symbols_s3_key:
            env_vars["SYMBOLS_S3_KEY"] = str(symbols_s3_key)

        symbols_s3_bucket = self.config.get("symbol_universe_s3_bucket")
        if not symbols_s3_bucket:
            symbols_s3_bucket = self.shared_storage.artifacts_bucket.bucket_name
        env_vars["SYMBOLS_S3_BUCKET"] = str(symbols_s3_bucket)

        function = PythonFunction(
            self,
            "DailyPricesIngestionOrchestrator",
            function_name=f"{self.env_name}-daily-prices-data-orchestrator",
            runtime=lambda_.Runtime.PYTHON_3_12,
            entry="src/lambda/functions/ingestion_orchestrator",
            index="handler.py",
            handler="main",
            memory_size=int(self.config.get("orchestrator_memory", 256)),
            timeout=Duration.seconds(int(self.config.get("orchestrator_timeout", 60))),
            log_retention=self._log_retention(),
            role=iam.Role.from_role_arn(self, "IngestionOrchestratorRole", self.lambda_execution_role_arn),
            layers=[self.common_layer],
            environment=env_vars,
        )
        return function

    def _create_batch_tracker_table(self) -> dynamodb.Table:
        """Create DynamoDB table to coordinate batch processing concurrency."""
        table_name = str(self.config.get("batch_tracker_table_name") or "").strip()
        if not table_name:
            table_name = f"{self.env_name}-daily-prices-batch-tracker"

        orchestration_mode = str(self.config.get("processing_orchestration_mode", "manual")).lower()
        enable_streams = orchestration_mode == "dynamodb_stream"

        table = dynamodb.Table(
            self,
            "BatchTrackerTable",
            table_name=table_name,
            partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="ttl",
            stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES if enable_streams else None,
        )

        removal_policy = str(self.config.get("removal_policy", "retain")).lower()
        if removal_policy == "destroy":
            table.apply_removal_policy(RemovalPolicy.DESTROY)
        else:
            table.apply_removal_policy(RemovalPolicy.RETAIN)

        return table

    def _deploy_symbol_universe_asset(self) -> None:
        """Deploy symbol universe file to the target S3 bucket via CDK asset."""

        asset_dir_raw = str(self.config.get("symbol_universe_asset_path", "") or "").strip()
        asset_file = str(self.config.get("symbol_universe_asset_file", "") or "").strip()
        s3_key_raw = str(self.config.get("symbol_universe_s3_key", "") or "").strip()

        if not asset_dir_raw or not asset_file or not s3_key_raw:
            return

        asset_dir = Path(asset_dir_raw)
        if not asset_dir.is_dir():
            raise FileNotFoundError(f"Symbol asset directory not found: {asset_dir}")

        source_file = asset_dir / asset_file
        if not source_file.is_file():
            raise FileNotFoundError(f"Symbol asset file not found: {source_file}")

        s3_key = PurePosixPath(s3_key_raw)
        destination_prefix = str(s3_key.parent) if str(s3_key.parent) != "." else ""

        bucket_name = str(self.config.get("symbol_universe_s3_bucket") or "").strip()
        if bucket_name:
            bucket = s3.Bucket.from_bucket_name(self, "SymbolUniverseBucket", bucket_name)
        else:
            bucket = self.shared_storage.artifacts_bucket

        s3_deployment.BucketDeployment(
            self,
            "SymbolUniverseDeployment",
            destination_bucket=bucket,
            destination_key_prefix=destination_prefix or None,
            sources=[s3_deployment.Source.asset(str(asset_dir))],
            prune=False,
        )

    def _create_queues(self) -> tuple[sqs.Queue, sqs.Queue]:
        """Create SQS DLQ and main queue for ingestion fan-out."""
        dlq = sqs.Queue(
            self,
            "IngestionDlq",
            queue_name=f"{self.env_name}-ingestion-dlq",
            retention_period=Duration.days(14),
            enforce_ssl=True,
        )

        # Visibility timeout aligned with worker timeout
        worker_timeout = int(self.config.get("worker_timeout", self.config.get("lambda_timeout", 300)))
        visibility = Duration.seconds(worker_timeout * 6)
        queue = sqs.Queue(
            self,
            "IngestionQueue",
            queue_name=f"{self.env_name}-ingestion-queue",
            visibility_timeout=visibility,
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=int(self.config.get("max_retries", 5)), queue=dlq),
            enforce_ssl=True,
        )
        return dlq, queue

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
        """Create scheduled trigger for daily prices data ingestion."""
        rule = events.Rule(
            self,
            "DailyPricesIngestionSchedule",
            rule_name=f"{self.env_name}-daily-prices-ingestion-schedule",
            schedule=events.Schedule.cron(
                minute="0",
                hour="22",  # Weekdays at 7 AM (KST)
                week_day="MON-FRI",
            ),
        )

        # Provide a default event payload so the ingestion can actually fetch data
        # Values are sourced from environment config with safe fallbacks
        default_event = self._default_ingestion_event()
        rule.add_target(
            targets.LambdaFunction(
                self.orchestrator_function,
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
            description="Daily prices ingestion function ARN",
        )

        CfnOutput(
            self,
            "OrchestratorFunctionArn",
            value=self.orchestrator_function.function_arn,
            description="Daily prices ingestion orchestrator function ARN",
        )

        CfnOutput(
            self,
            "IngestionQueueUrl",
            value=self.queue.queue_url,
            description="Ingestion SQS queue URL",
        )

    def _create_alarms_and_dashboard(self) -> None:
        # SQS queue depth alarm
        depth_metric = self.queue.metric_approximate_number_of_messages_visible(
            period=Duration.minutes(5),
            statistic="Average",
        )
        cw.Alarm(
            self,
            "IngestionQueueDepthAlarm",
            alarm_description="Ingestion queue depth high",
            metric=depth_metric,
            threshold=float(self.config.get("alarm_queue_depth_threshold", 100.0)),
            evaluation_periods=1,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        )

        # Oldest message age alarm
        age_metric = self.queue.metric_approximate_age_of_oldest_message(period=Duration.minutes(5))
        cw.Alarm(
            self,
            "IngestionQueueAgeAlarm",
            alarm_description="Old messages in ingestion queue",
            metric=age_metric,
            threshold=float(self.config.get("alarm_queue_age_seconds", 300.0)),
            evaluation_periods=1,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        )

        # Worker Lambda error/throttle alarms
        errors_metric = self.ingestion_function.metric_errors(period=Duration.minutes(5))
        throttles_metric = self.ingestion_function.metric_throttles(period=Duration.minutes(5))

        cw.Alarm(
            self,
            "IngestionWorkerErrorsAlarm",
            alarm_description="Ingestion worker errors detected",
            metric=errors_metric,
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        )
        cw.Alarm(
            self,
            "IngestionWorkerThrottlesAlarm",
            alarm_description="Ingestion worker throttles detected",
            metric=throttles_metric,
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        )

        # Dashboard
        dash = cw.Dashboard(self, "IngestionDashboard", dashboard_name=f"{self.env_name}-ingestion-dashboard")
        dash.add_widgets(
            cw.GraphWidget(title="SQS Depth", left=[depth_metric]),
            cw.GraphWidget(title="SQS Oldest Age", left=[age_metric]),
            cw.GraphWidget(title="Worker Errors/Throttles", left=[errors_metric, throttles_metric]),
        )

    def _create_common_layer(self) -> lambda_.LayerVersion:
        """Create Common Layer for shared models and utilities.

        Uses standard Python layer layout: python/shared/... at the root of asset.
        """
        return PythonLayerVersion(
            self,
            "CommonLayer",
            entry="src/lambda/layers/common",
            layer_version_name=f"{self.env_name}-common-layer",
            description="Shared common models and utilities",
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            bundling=BundlingOptions(
                command=[
                    "bash",
                    "-c",
                    "set -euxo pipefail; "
                    "mkdir -p /asset-output/python; "
                    "cp -R /asset-input/python/. /asset-output/python/; "
                    "if [ -f requirements.txt ]; then pip install -q -r requirements.txt -t /asset-output/python; fi",
                ],
                asset_excludes=["tests", "__pycache__", "*.pyc"],
            ),
        )

    def _create_market_domain_layer(self) -> lambda_.LayerVersion:
        """Create market domain layer bundling ingestion services and providers."""

        return PythonLayerVersion(
            self,
            "MarketDomainLayer",
            entry="src/lambda/layers/data/market",
            layer_version_name=f"{self.env_name}-market-domain-layer",
            description="Market domain shared logic (ingestion, validation)",
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            bundling=BundlingOptions(
                command=[
                    "bash",
                    "-c",
                    "set -euxo pipefail; "
                    "mkdir -p /asset-output/python; "
                    "cp -R /asset-input/python/. /asset-output/python/; "
                    "if [ -f requirements.txt ]; then pip install -q -r requirements.txt -t /asset-output/python; fi",
                ],
                asset_excludes=["tests", "__pycache__", "*.pyc"],
            ),
        )

    def _create_market_data_dependencies_layer(self) -> lambda_.LayerVersion:
        """Create a dedicated layer for market data third-party dependencies.

        Best practice: keep heavy deps (yfinance/pandas/numpy) in a separate layer
        to avoid fat Lambda packages and speed up cold starts.
        """
        return lambda_.LayerVersion(
            self,
            "MarketDataDependenciesLayer",
            layer_version_name=f"{self.env_name}-market-data-dependencies",
            description="Third-party dependencies for market data (yfinance, pandas, etc.)",
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            code=lambda_.Code.from_asset("src/lambda/layers/data/market/dependencies"),
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
        trigger_type = self.config.get("ingestion_trigger_type", "schedule")

        return {
            "data_source": self.data_source,
            "data_type": "prices",
            "domain": domain,
            "table_name": table_name,
            "symbols": symbols,
            "period": period,
            "interval": interval,
            "file_format": file_format,
            "trigger_type": trigger_type,
        }
