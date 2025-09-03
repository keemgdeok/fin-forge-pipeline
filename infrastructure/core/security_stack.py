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
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.env_name = environment
        self.config = config

        # Core execution roles
        self.lambda_execution_role = self._create_lambda_execution_role()
        self.glue_execution_role = self._create_glue_execution_role()
        self.step_functions_execution_role = (
            self._create_step_functions_execution_role()
        )

        # GitHub Actions OIDC provider and deploy role
        # This enables CI/CD via GitHub Actions without long-lived keys.
        self.github_oidc_provider = self._create_github_oidc_provider()
        self.github_actions_deploy_role = self._create_github_actions_deploy_role()

        self._create_outputs()

    def _create_lambda_execution_role(self) -> iam.Role:
        """Create base Lambda execution role."""
        return iam.Role(
            self,
            "LambdaExecutionRole",
            role_name=f"{self.env_name}-data-platform-lambda-role",
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
                            resources=[
                                "*"
                            ],  # Will be restricted by pipeline-specific policies
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
            role_name=f"{self.env_name}-data-platform-glue-role",
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
            role_name=f"{self.env_name}-data-platform-stepfunctions-role",
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

        CfnOutput(
            self,
            "GitHubActionsDeployRoleArn",
            value=self.github_actions_deploy_role.role_arn,
            description="GitHub Actions OIDC deploy role ARN",
        )

    def _create_github_oidc_provider(self) -> iam.OpenIdConnectProvider:
        """Create (or define) the GitHub OIDC provider.

        Note: OIDC providers are global in IAM. Creating this via CDK will
        manage it in this account; if one already exists with the same URL,
        consider importing instead.
        """
        return iam.OpenIdConnectProvider(
            self,
            "GitHubOidcProvider",
            url="https://token.actions.githubusercontent.com",
            client_ids=["sts.amazonaws.com"],
        )

    def _create_github_actions_deploy_role(self) -> iam.Role:
        """Role assumed by GitHub Actions via OIDC for deployments.

        Trust is restricted to the repository and common branches/environments.
        For stricter control, adjust the conditions below.
        """
        # Scope to your repository: owner/repo
        repo_owner = "keemgdeok"
        repo_name = "finge"

        principal = iam.OpenIdConnectPrincipal(
            self.github_oidc_provider,
            conditions={
                "StringEquals": {
                    "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
                },
                "StringLike": {
                    "token.actions.githubusercontent.com:sub": [
                        f"repo:{repo_owner}/{repo_name}:ref:refs/heads/main",
                        f"repo:{repo_owner}/{repo_name}:ref:refs/heads/develop",
                        f"repo:{repo_owner}/{repo_name}:environment:prod",
                        f"repo:{repo_owner}/{repo_name}:environment:dev",
                    ]
                },
            },
        )

        # Start with AdministratorAccess for simplicity in small teams; tighten later.
        return iam.Role(
            self,
            "GitHubActionsDeployRole",
            role_name=f"{self.env_name}-github-actions-deploy-role",
            assumed_by=principal,
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AdministratorAccess"
                )
            ],
        )
