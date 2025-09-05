"""Customer data processing pipeline stack."""

from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_logs as logs,
    Duration,
    CfnOutput,
)
from constructs import Construct
from aws_cdk.aws_lambda_python_alpha import PythonFunction


class CustomerDataProcessingStack(Stack):
    """Customer data ETL processing pipeline."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: str,
        config: dict,
        shared_storage_stack,
        lambda_execution_role_arn: str,
        glue_execution_role_arn: str,
        step_functions_execution_role_arn: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.env_name = environment
        self.config = config
        self.shared_storage = shared_storage_stack
        self.lambda_execution_role_arn = lambda_execution_role_arn
        self.glue_execution_role_arn = glue_execution_role_arn
        self.step_functions_execution_role_arn = step_functions_execution_role_arn

        # Deterministic Glue job name used across resources (avoids Optional[str] typing)
        self.etl_job_name: str = f"{self.env_name}-customer-data-etl"

        # Common Layer for shared modules
        self.common_layer = self._create_common_layer()

        # Glue ETL job for customer data transformation
        self.etl_job = self._create_etl_job()

        # Feature toggle: processing orchestration (SFN + EB Rule)
        self.enable_processing: bool = bool(self.config.get("enable_processing_orchestration", False))

        if self.enable_processing:
            # Step Functions workflow for orchestration
            self.processing_workflow = self._create_processing_workflow()
            # EventBridge rules: S3 Object Created -> Start State Machine (multi prefix/suffix)
            self.s3_to_sfn_rules = self._create_s3_to_sfn_rules()

        self._create_outputs()

    def _create_etl_job(self) -> glue.CfnJob:
        """Create Glue ETL job for customer data processing."""
        return glue.CfnJob(
            self,
            "CustomerETLJob",
            name=self.etl_job_name,
            role=self.glue_execution_role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=(
                    f"s3://{self.shared_storage.artifacts_bucket.bucket_name}" "/glue-scripts/customer_data_etl.py"
                ),  # TODO: Upload script in Phase 2
                python_version="3",
            ),
            default_arguments={
                "--job-language": "python",
                "--job-bookmark-option": "job-bookmark-enable",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--TempDir": (f"s3://{self.shared_storage.artifacts_bucket.bucket_name}" "/temp/"),
                "--raw_bucket": self.shared_storage.raw_bucket.bucket_name,
                "--curated_bucket": self.shared_storage.curated_bucket.bucket_name,
                "--environment": self.env_name,
            },
            glue_version="4.0",
            max_retries=2,
            timeout=60,
            worker_type="G.1X",
            number_of_workers=int(self.config.get("glue_max_capacity", 2)),
        )

    def _create_processing_workflow(self) -> sfn.StateMachine:
        """Create Step Functions workflow for customer data processing."""
        # Data validation task
        validate_data_task = tasks.LambdaInvoke(
            self,
            "ValidateCustomerData",
            lambda_function=self._create_validation_function(),
            output_path="$.Payload",
        )

        # ETL job task
        etl_task = tasks.GlueStartJobRun(
            self,
            "ProcessCustomerData",
            glue_job_name=self.etl_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
        )

        # Data quality check task
        quality_check_task = tasks.LambdaInvoke(
            self,
            "QualityCheckCustomerData",
            lambda_function=self._create_quality_check_function(),
            output_path="$.Payload",
        )

        # Success notification
        success_task = sfn.Succeed(
            self,
            "CustomerDataProcessingSuccess",
            comment="Customer data processing completed successfully",
        )

        # Error handling
        error_task = sfn.Fail(
            self,
            "CustomerDataProcessingFailed",
            comment="Customer data processing failed",
        )

        # Define workflow
        definition = validate_data_task.next(
            sfn.Choice(self, "ValidationChoice")
            .when(
                sfn.Condition.boolean_equals("$.validation_passed", True),
                etl_task.next(
                    quality_check_task.next(
                        sfn.Choice(self, "QualityChoice")
                        .when(
                            sfn.Condition.boolean_equals("$.quality_passed", True),
                            success_task,
                        )
                        .otherwise(error_task)
                    )
                ),
            )
            .otherwise(error_task)
        )

        # Create log group for the state machine with retention from config
        sm_log_group = logs.LogGroup(
            self,
            "ProcessingStateMachineLogs",
            retention=self._log_retention(),
        )

        return sfn.StateMachine(
            self,
            "CustomerDataProcessingWorkflow",
            state_machine_name=f"{self.env_name}-customer-data-processing",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            role=iam.Role.from_role_arn(
                self,
                "StepFunctionsExecutionRoleRef",
                self.step_functions_execution_role_arn,
            ),
            logs=sfn.LogOptions(destination=sm_log_group, level=sfn.LogLevel.ALL, include_execution_data=True),
            tracing_enabled=bool(self.config.get("enable_xray_tracing", False)),
            timeout=Duration.hours(2),
        )

    def _create_s3_to_sfn_rules(self) -> list[events.Rule]:
        """Create one or more EventBridge rules to invoke the state machine.

        Supports multi-domain/table by accepting a list of trigger configs in env config
        under key 'processing_triggers'. Falls back to a single trigger derived from
        ingestion defaults if not provided.
        """
        triggers: list[dict] = list(self.config.get("processing_triggers", []))

        if not triggers:
            triggers = [
                {
                    "domain": self.config.get("ingestion_domain", "market"),
                    "table_name": self.config.get("ingestion_table_name", "prices"),
                    "file_type": self.config.get("ingestion_file_format", "json"),
                    "suffixes": self.config.get("processing_suffixes", [".json", ".csv"]),
                }
            ]

        created: list[events.Rule] = []
        for t in triggers:
            domain: str = str(t.get("domain", "")).strip()
            table_name: str = str(t.get("table_name", "")).strip()
            file_type: str = str(t.get("file_type", "json")).strip() or "json"
            suffixes: list[str] = list(t.get("suffixes", [".json", ".csv"]))

            if not domain or not table_name:
                # Skip malformed trigger
                continue

            rule = self._add_s3_to_sfn_rule(domain, table_name, file_type, suffixes)
            created.append(rule)

        return created

    def _add_s3_to_sfn_rule(self, domain: str, table_name: str, file_type: str, suffixes: list[str]) -> events.Rule:
        """Create a single EventBridge rule for a specific domain/table and suffix set."""
        prefix = f"{domain}/{table_name}/"

        # Build key filters: require prefix AND any-of suffixes, use one entry per suffix
        key_filters = [{"prefix": prefix, "suffix": s} for s in suffixes]

        rule_id = f"RawObjectCreated-{domain}-{table_name}".replace("/", "-")
        rule = events.Rule(
            self,
            rule_id,
            rule_name=f"{self.env_name}-raw-object-created-{domain}-{table_name}",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [self.shared_storage.raw_bucket.bucket_name]},
                    "object": {"key": key_filters},
                },
            ),
        )

        rule.add_target(
            targets.SfnStateMachine(
                self.processing_workflow,
                input=events.RuleTargetInput.from_object(
                    {
                        "source_bucket": events.EventField.from_path("$.detail.bucket.name"),
                        "source_key": events.EventField.from_path("$.detail.object.key"),
                        "table_name": table_name,
                        "domain": domain,
                        "file_type": file_type,
                    }
                ),
            )
        )

        return rule

    def _create_validation_function(self) -> lambda_.IFunction:
        """Create data validation Lambda function wired to real handler."""
        function = PythonFunction(
            self,
            "CustomerDataValidationFunction",
            function_name=f"{self.env_name}-customer-data-validation",
            runtime=lambda_.Runtime.PYTHON_3_12,
            entry="src/lambda/functions/data_validator",
            index="handler.py",
            handler="main",
            memory_size=self._lambda_memory(),
            timeout=self._lambda_timeout(),
            log_retention=self._log_retention(),
            role=iam.Role.from_role_arn(self, "ValidationLambdaRole", self.lambda_execution_role_arn),
            layers=[self.common_layer],
            environment={
                "ENVIRONMENT": self.env_name,
                "RAW_BUCKET": self.shared_storage.raw_bucket.bucket_name,
            },
        )

        # S3 권한은 SecurityStack의 LambdaExecutionRole에 최소권한으로 부여됨
        # (교차 스택 grant로 인한 순환 참조를 피하기 위해 여기서는 grant를 사용하지 않음)
        return function

    def _create_quality_check_function(self) -> lambda_.Function:
        """Create data quality check Lambda function."""
        function = lambda_.Function(
            self,
            "CustomerDataQualityCheckFunction",
            function_name=f"{self.env_name}-customer-data-quality-check",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_inline(
                "def lambda_handler(event, context): return {'quality_passed': True}"
            ),  # Placeholder
            memory_size=self._lambda_memory(),
            timeout=self._lambda_timeout(),
            log_retention=self._log_retention(),
            role=iam.Role.from_role_arn(self, "QualityCheckLambdaRole", self.lambda_execution_role_arn),
            layers=[self.common_layer],
            environment={
                "ENVIRONMENT": self.env_name,
                "CURATED_BUCKET": self.shared_storage.curated_bucket.bucket_name,
            },
        )

        # S3 권한은 SecurityStack의 LambdaExecutionRole에 최소권한으로 부여됨
        # (교차 스택 grant로 인한 순환 참조를 피하기 위해 여기서는 grant를 사용하지 않음)
        return function

    def _create_outputs(self) -> None:
        """Create CloudFormation outputs."""
        CfnOutput(
            self,
            "ETLJobName",
            value=self.etl_job_name,
            description="Customer data ETL job name",
        )

        # Output only when processing is enabled
        if getattr(self, "enable_processing", False) and hasattr(self, "processing_workflow"):
            CfnOutput(
                self,
                "ProcessingWorkflowArn",
                value=self.processing_workflow.state_machine_arn,
                description="Customer data processing workflow ARN",
            )

    def _create_common_layer(self) -> lambda_.LayerVersion:
        """Create Common Layer for shared models and utils."""
        return lambda_.LayerVersion(
            self,
            "CommonLayer",
            layer_version_name=f"{self.env_name}-common-layer",
            description="Shared common models and utilities",
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            code=lambda_.Code.from_asset("src/lambda/layers/common"),
        )

    # ===== Helpers =====
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
