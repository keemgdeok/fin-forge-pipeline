"""Customer data processing pipeline stack."""
from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_lambda as lambda_,
    Duration,
    CfnOutput,
)
from constructs import Construct


class CustomerDataProcessingStack(Stack):
    """Customer data ETL processing pipeline."""

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
            name=f"{self.environment}-customer-data-etl",
            role=self.security.glue_execution_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{self.shared_storage.artifacts_bucket.bucket_name}/glue-scripts/customer_data_etl.py",  # TODO: Upload script in Phase 2
                python_version="3",
            ),
            default_arguments={
                "--job-language": "python",
                "--job-bookmark-option": "job-bookmark-enable",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--TempDir": f"s3://{self.shared_storage.artifacts_bucket.bucket_name}/temp/",
                "--raw_bucket": self.shared_storage.raw_bucket.bucket_name,
                "--curated_bucket": self.shared_storage.curated_bucket.bucket_name,
                "--environment": self.environment,
            },
            glue_version="4.0",
            max_retries=2,
            timeout=60,
            worker_type="G.1X",
            number_of_workers=2,
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
            glue_job_name=self.etl_job.name,
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
        definition = (
            validate_data_task
            .next(
                sfn.Choice(self, "ValidationChoice")
                .when(
                    sfn.Condition.boolean_equals("$.validation_passed", True),
                    etl_task.next(
                        quality_check_task.next(
                            sfn.Choice(self, "QualityChoice")
                            .when(
                                sfn.Condition.boolean_equals("$.quality_passed", True),
                                success_task
                            )
                            .otherwise(error_task)
                        )
                    )
                )
                .otherwise(error_task)
            )
        )

        return sfn.StateMachine(
            self,
            "CustomerDataProcessingWorkflow",
            state_machine_name=f"{self.environment}-customer-data-processing",
            definition=definition,
            role=self.security.step_functions_execution_role,
            timeout=Duration.hours(2),
        )

    def _create_validation_function(self) -> lambda_.Function:
        """Create data validation Lambda function."""
        function = lambda_.Function(
            self,
            "CustomerDataValidationFunction",
            function_name=f"{self.environment}-customer-data-validation",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_inline(
                "def lambda_handler(event, context): return {'validation_passed': True}"
            ),  # Placeholder
            timeout=Duration.minutes(3),
            role=self.security.lambda_execution_role,
            environment={
                "ENVIRONMENT": self.environment,
                "RAW_BUCKET": self.shared_storage.raw_bucket.bucket_name,
            },
        )

        self.shared_storage.raw_bucket.grant_read(function)
        return function

    def _create_quality_check_function(self) -> lambda_.Function:
        """Create data quality check Lambda function."""
        function = lambda_.Function(
            self,
            "CustomerDataQualityCheckFunction", 
            function_name=f"{self.environment}-customer-data-quality-check",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_inline(
                "def lambda_handler(event, context): return {'quality_passed': True}"
            ),  # Placeholder
            timeout=Duration.minutes(3),
            role=self.security.lambda_execution_role,
            environment={
                "ENVIRONMENT": self.environment,
                "CURATED_BUCKET": self.shared_storage.curated_bucket.bucket_name,
            },
        )

        self.shared_storage.curated_bucket.grant_read(function)
        return function

    def _create_outputs(self) -> None:
        """Create CloudFormation outputs."""
        CfnOutput(
            self,
            "ETLJobName",
            value=self.etl_job.name,
            description="Customer data ETL job name",
        )

        CfnOutput(
            self,
            "ProcessingWorkflowArn",
            value=self.processing_workflow.state_machine_arn,
            description="Customer data processing workflow ARN",
        )
