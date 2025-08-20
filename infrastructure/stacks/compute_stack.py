"""Compute stack for Lambda functions and Glue jobs."""
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_glue as glue,
    aws_logs as logs,
    Duration,
)
from aws_cdk.aws_lambda_python_alpha import PythonFunction, PythonLayerVersion
from constructs import Construct
import os


class ComputeStack(Stack):
    """Stack containing Lambda functions and Glue jobs."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: str,
        config: dict,
        storage_stack,
        analytics_stack,
        security_stack,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.environment = environment
        self.config = config
        self.storage_stack = storage_stack
        self.analytics_stack = analytics_stack
        self.security_stack = security_stack

        # Create Lambda layers
        self._create_lambda_layers()

        # Create Lambda functions
        self._create_lambda_functions()

        # Create Glue job
        self._create_glue_job()

    def _create_lambda_layers(self) -> None:
        """Create Lambda layers for shared code."""
        self.common_layer = PythonLayerVersion(
            self, "CommonUtilsLayer",
            entry="src/lambda/shared/layers/common_utils",
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_12],
            description="Common utilities layer",
        )

        self.data_processing_layer = PythonLayerVersion(
            self, "DataProcessingLayer", 
            entry="src/lambda/shared/layers/data_processing",
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_12],
            description="Data processing utilities layer",
        )

    def _create_lambda_functions(self) -> None:
        """Create Lambda functions."""
        self.lambda_functions = {}

        function_configs = [
            "data_ingestion",
            "glue_etl_trigger", 
            "data_transformation",
            "glue_crawler_manager",
            "athena_query_executor",
            "notification_handler",
            "error_handler",
        ]

        common_env = {
            "ENVIRONMENT": self.environment,
            "RAW_BUCKET": self.storage_stack.raw_bucket.bucket_name,
            "CURATED_BUCKET": self.storage_stack.curated_bucket.bucket_name,
            "GLUE_DATABASE": self.analytics_stack.glue_database.ref,
        }

        for func_name in function_configs:
            self.lambda_functions[func_name] = PythonFunction(
                self, f"{func_name.replace('_', '').title()}Function",
                entry=f"src/lambda/{func_name}",
                runtime=_lambda.Runtime.PYTHON_3_12,
                handler="handler.main",
                layers=[self.common_layer, self.data_processing_layer],
                timeout=Duration.seconds(self.config.get("lambda_timeout", 300)),
                memory_size=self.config.get("lambda_memory", 512),
                environment=common_env,
                tracing=_lambda.Tracing.ACTIVE if self.config.get("enable_xray_tracing") else _lambda.Tracing.DISABLED,
                log_retention=getattr(logs.RetentionDays, f"DAYS_{self.config.get('log_retention_days', 14)}"),
                role=self.security_stack.lambda_execution_role,
            )

            # Grant permissions to buckets
            self.storage_stack.raw_bucket.grant_read_write(self.lambda_functions[func_name])
            self.storage_stack.curated_bucket.grant_read_write(self.lambda_functions[func_name])

    def _create_glue_job(self) -> None:
        """Create Glue ETL job."""
        self.glue_job = glue.CfnJob(
            self, "EtlJob",
            name=f"data-pipeline-etl-{self.environment}",
            role=self.security_stack.glue_job_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{self.storage_stack.curated_bucket.bucket_name}/scripts/etl_raw_to_curated.py",
                python_version="3",
            ),
            default_arguments={
                "--TempDir": f"s3://{self.storage_stack.curated_bucket.bucket_name}/temp/",
                "--job-bookmark-option": "job-bookmark-enable",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--RAW_BUCKET": self.storage_stack.raw_bucket.bucket_name,
                "--CURATED_BUCKET": self.storage_stack.curated_bucket.bucket_name,
            },
            max_capacity=self.config.get("glue_max_capacity", 2),
            timeout=2880,  # 48 hours
        )