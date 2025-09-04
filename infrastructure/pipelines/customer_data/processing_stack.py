"""Customer data processing pipeline stack."""

from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
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

        # Step Functions workflow for orchestration
        self.processing_workflow = self._create_processing_workflow()

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
