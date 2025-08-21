"""Security foundation stack for serverless data platform."""
from aws_cdk import (
    Stack,
    aws_iam as iam,
    CfnOutput,
)
from constructs import Construct


class SecurityStack(Stack):
    """Central security stack managing all IAM roles and policies."""

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

        # Core execution roles
        self.lambda_execution_role = self._create_lambda_execution_role()
        self.glue_execution_role = self._create_glue_execution_role()
        self.step_functions_execution_role = self._create_step_functions_execution_role()

        self._create_outputs()

    def _create_lambda_execution_role(self) -> iam.Role:
        """Create base Lambda execution role."""
        return iam.Role(
            self,
            "LambdaExecutionRole",
            role_name=f"{self.environment}-data-platform-lambda-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
            ],
            inline_policies={
                "S3Access": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "s3:GetObject",
                                "s3:PutObject", 
                                "s3:DeleteObject",
                                "s3:ListBucket",
                            ],
                            resources=["*"],  # Will be restricted by pipeline-specific policies
                        ),
                    ]
                ),
                "GlueAccess": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "glue:StartJobRun",
                                "glue:GetJobRun",
                                "glue:GetJobRuns",
                                "glue:StartCrawler",
                                "glue:GetCrawler",
                                "glue:GetCrawlerMetrics",
                            ],
                            resources=["*"],
                        ),
                    ]
                ),
            },
        )

    def _create_glue_execution_role(self) -> iam.Role:
        """Create Glue job execution role."""
        return iam.Role(
            self,
            "GlueExecutionRole",
            role_name=f"{self.environment}-data-platform-glue-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
            ],
            inline_policies={
                "S3DataAccess": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "s3:GetObject",
                                "s3:PutObject",
                                "s3:DeleteObject",
                                "s3:ListBucket",
                            ],
                            resources=["*"],
                        ),
                    ]
                ),
            },
        )

    def _create_step_functions_execution_role(self) -> iam.Role:
        """Create Step Functions execution role."""
        return iam.Role(
            self,
            "StepFunctionsExecutionRole",
            role_name=f"{self.environment}-data-platform-stepfunctions-role",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
            inline_policies={
                "LambdaInvoke": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["lambda:InvokeFunction"],
                            resources=["*"],
                        ),
                    ]
                ),
                "GlueJobManagement": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "glue:StartJobRun",
                                "glue:GetJobRun",
                                "glue:BatchStopJobRun",
                            ],
                            resources=["*"],
                        ),
                    ]
                ),
            },
        )

    def _create_outputs(self) -> None:
        """Create CloudFormation outputs."""
        CfnOutput(
            self,
            "LambdaExecutionRoleArn",
            value=self.lambda_execution_role.role_arn,
            description="Lambda execution role ARN",
        )

        CfnOutput(
            self,
            "GlueExecutionRoleArn",
            value=self.glue_execution_role.role_arn,
            description="Glue execution role ARN",
        )

        CfnOutput(
            self,
            "StepFunctionsExecutionRoleArn",
            value=self.step_functions_execution_role.role_arn,
            description="Step Functions execution role ARN",
        )