"""Security stack for IAM roles and policies."""
from aws_cdk import (
    Stack,
    aws_iam as iam,
)
from constructs import Construct


class SecurityStack(Stack):
    """Stack containing IAM roles and policies."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: str,
        config: dict,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.environment = environment
        self.config = config

        # Create IAM roles
        self._create_lambda_execution_role()
        self._create_glue_job_role()
        self._create_step_function_role()

    def _create_lambda_execution_role(self) -> None:
        """Create IAM role for Lambda functions."""
        self.lambda_execution_role = iam.Role(
            self, "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AWSXRayDaemonWriteAccess"
                ),
            ],
        )

    def _create_glue_job_role(self) -> None:
        """Create IAM role for Glue jobs."""
        self.glue_job_role = iam.Role(
            self, "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
            ],
        )

    def _create_step_function_role(self) -> None:
        """Create IAM role for Step Functions."""
        self.step_function_role = iam.Role(
            self, "StepFunctionRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaRole"
                ),
            ],
        )