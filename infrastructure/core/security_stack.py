"""Security foundation stack for serverless data platform."""

import os
from typing import Iterable, Optional, Sequence

from aws_cdk import ArnFormat, Stack, aws_iam as iam, CfnOutput
from constructs import Construct

from infrastructure.config.types import EnvironmentConfig
from infrastructure.core.iam import utils as iam_utils
from infrastructure.core.iam.glue_execution_role import GlueExecutionRoleConstruct
from infrastructure.core.iam.lambda_execution_role import LambdaExecutionRoleConstruct
from infrastructure.core.shared_storage_stack import SharedStorageStack


class SecurityStack(Stack):
    """Central security stack managing all IAM roles and policies."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: str,
        config: EnvironmentConfig,
        shared_storage_stack: SharedStorageStack,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.env_name = environment
        self.config: EnvironmentConfig = config
        self.shared_storage = shared_storage_stack

        # Core execution roles
        lambda_role_construct = LambdaExecutionRoleConstruct(
            self,
            "LambdaExecutionRole",
            env_name=self.env_name,
            config=self.config,
            raw_bucket=self.shared_storage.raw_bucket,
            curated_bucket=self.shared_storage.curated_bucket,
            artifacts_bucket=self.shared_storage.artifacts_bucket,
            # batch_tracker_table=self.shared_storage.batch_tracker_table,
        )
        self.lambda_execution_role = lambda_role_construct.role

        glue_role_construct = GlueExecutionRoleConstruct(
            self,
            "GlueExecutionRole",
            env_name=self.env_name,
            config=self.config,
            raw_bucket=self.shared_storage.raw_bucket,
            curated_bucket=self.shared_storage.curated_bucket,
            artifacts_bucket=self.shared_storage.artifacts_bucket,
        )
        self.glue_execution_role = glue_role_construct.role

        self.step_functions_execution_role = self._create_step_functions_execution_role()

        # GitHub Actions OIDC provider and deploy role
        # This enables CI/CD via GitHub Actions without long-lived keys.
        self.github_oidc_provider = self._create_github_oidc_provider()
        self.github_actions_deploy_role = self._create_github_actions_deploy_role()

        self._create_outputs()

    def _config_string_list(self, key: str, default: Sequence[str]) -> list[str]:
        """Return a normalized list[str] from configuration or fall back to default."""
        return iam_utils.config_string_list(self.config, key, default)

    def _namespaced_resource_names(self, names: Iterable[str]) -> list[str]:
        """Ensure names are prefixed with the environment namespace."""
        prefix = f"{self.env_name}-"
        scoped: list[str] = []
        for raw_name in names:
            name = str(raw_name or "").strip()
            if not name:
                continue
            scoped.append(name if name.startswith(prefix) else f"{prefix}{name}")
        return scoped

    def _lambda_function_arn(self, function_name: str) -> str:
        """Build colon-delimited Lambda ARN (avoids format regressions)."""
        return self.format_arn(
            service="lambda",
            resource="function",
            arn_format=ArnFormat.COLON_RESOURCE_NAME,
            resource_name=function_name,
        )

    def _glue_job_arn(self, job_name: str) -> str:
        """Build Glue job ARN."""
        return self.format_arn(
            service="glue",
            resource="job",
            arn_format=ArnFormat.SLASH_RESOURCE_NAME,
            resource_name=job_name,
        )

    def _glue_crawler_arn(self, crawler_name: str) -> str:
        """Build Glue crawler ARN."""
        return self.format_arn(
            service="glue",
            resource="crawler",
            arn_format=ArnFormat.SLASH_RESOURCE_NAME,
            resource_name=crawler_name,
        )

    def _step_functions_lambda_functions(self) -> list[str]:
        """Resolve Lambda function names the Step Functions role must invoke."""
        names = self._config_string_list(
            "step_functions_lambda_functions",
            default=[
                "daily-prices-data-preflight",
                "schema-change-decider",
                "daily-prices-compaction-guard",
            ],
        )
        return self._namespaced_resource_names(names)

    def _step_functions_glue_jobs(self) -> list[str]:
        """Resolve Glue job names the Step Functions role must manage."""
        names = self._config_string_list(
            "step_functions_glue_jobs",
            default=[
                "daily-prices-data-etl",
                "daily-prices-compaction",
            ],
        )
        return self._namespaced_resource_names(names)

    def _step_functions_glue_crawlers(self) -> list[str]:
        """Resolve Glue crawlers triggered by Step Functions workflows."""
        names = self._config_string_list(
            "step_functions_glue_crawlers",
            default=[
                "curated-data-crawler",
            ],
        )
        return self._namespaced_resource_names(names)

    def _create_step_functions_execution_role(self) -> iam.Role:
        """Create Step Functions execution role."""
        lambda_function_arns = [self._lambda_function_arn(name) for name in self._step_functions_lambda_functions()]
        glue_job_arns = [self._glue_job_arn(name) for name in self._step_functions_glue_jobs()]
        crawler_arns = [self._glue_crawler_arn(name) for name in self._step_functions_glue_crawlers()]

        inline_policies: dict[str, iam.PolicyDocument] = {}

        if lambda_function_arns:
            inline_policies["LambdaInvoke"] = iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=["lambda:InvokeFunction"],
                        resources=lambda_function_arns,
                    ),
                ]
            )

        if glue_job_arns:
            inline_policies["GlueJobManagement"] = iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "glue:StartJobRun",
                            "glue:GetJobRun",
                            "glue:BatchStopJobRun",
                        ],
                        resources=glue_job_arns,
                    ),
                ]
            )

        if crawler_arns:
            inline_policies["GlueCrawlerManagement"] = iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "glue:StartCrawler",
                            "glue:GetCrawler",
                        ],
                        resources=crawler_arns,
                    ),
                ]
            )

        inline_policies["CloudWatchLogsDelivery"] = iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "logs:CreateLogDelivery",
                        "logs:GetLogDelivery",
                        "logs:UpdateLogDelivery",
                        "logs:DeleteLogDelivery",
                        "logs:ListLogDeliveries",
                        "logs:PutResourcePolicy",
                        "logs:DescribeResourcePolicies",
                        "logs:DescribeLogGroups",
                        "logs:DescribeLogStreams",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents",
                    ],
                    resources=["*"],
                )
            ]
        )

        return iam.Role(
            self,
            "StepFunctionsExecutionRole",
            role_name=f"{self.env_name}-data-platform-stepfunctions-role",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
            inline_policies=inline_policies,
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

    # Decide whether to reuse, default, or create the GitHub Actions OIDC provider.
    def _create_github_oidc_provider(self) -> iam.IOpenIdConnectProvider:
        """Import an existing GitHub OIDC provider or create a new one.

        OIDC providers are account-wide resources. When one already exists, re-use it
        to avoid ``EntityAlreadyExistsException`` during deployments. Resolution order:

        1. Explicit config value ``config["github_oidc_provider_arn"]``
        2. CDK context value ``githubOidcProviderArn``
        3. Environment variable ``GITHUB_OIDC_PROVIDER_ARN``

        If no ARN is supplied, a new provider is created (fresh accounts).
        """

        existing_provider_arn = self._resolve_github_oidc_provider_arn()
        if existing_provider_arn:
            return iam.OpenIdConnectProvider.from_open_id_connect_provider_arn(
                self,
                "GitHubOidcProvider",
                existing_provider_arn,
            )

        if not self._should_create_github_oidc_provider():
            default_provider_arn = self._default_github_oidc_provider_arn()
            return iam.OpenIdConnectProvider.from_open_id_connect_provider_arn(
                self,
                "GitHubOidcProviderDefault",
                default_provider_arn,
            )

        return iam.OpenIdConnectProvider(
            self,
            "GitHubOidcProvider",
            url="https://token.actions.githubusercontent.com",
            client_ids=["sts.amazonaws.com"],
        )

    # Pull an explicit GitHub OIDC provider ARN from config, context, or environment.
    def _resolve_github_oidc_provider_arn(self) -> Optional[str]:
        """Resolve GitHub OIDC provider ARN from config, context, or environment."""

        config_arn_raw = self.config.get("github_oidc_provider_arn")
        if isinstance(config_arn_raw, str) and config_arn_raw.strip():
            return config_arn_raw.strip()

        context_arn = self.node.try_get_context("githubOidcProviderArn")
        if isinstance(context_arn, str) and context_arn.strip():
            return context_arn.strip()

        env_arn = os.getenv("GITHUB_OIDC_PROVIDER_ARN", "")
        if env_arn.strip():
            return env_arn.strip()

        return None

    # Check deployment knobs to see if a new GitHub OIDC provider should be created.
    def _should_create_github_oidc_provider(self) -> bool:
        """Determine whether a new GitHub OIDC provider should be created."""

        def _to_bool(value: object) -> bool:
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                normalized = value.strip().lower()
                return normalized in {"1", "true", "yes", "y"}
            return False

        config_flag = _to_bool(self.config.get("github_oidc_provider_create"))
        if config_flag:
            return True

        context_flag = _to_bool(self.node.try_get_context("githubOidcProviderCreate"))
        if context_flag:
            return True

        env_flag = _to_bool(os.getenv("GITHUB_OIDC_PROVIDER_CREATE"))
        return env_flag

    # Build the canonical ARN for the token.actions.githubusercontent.com provider in this account.
    def _default_github_oidc_provider_arn(self) -> str:
        """Return the standard ARN for the GitHub Actions OIDC provider in this account."""

        return self.format_arn(
            service="iam",
            region="",
            resource="oidc-provider/token.actions.githubusercontent.com",
        )

    # Define the GitHub Actions deploy role that trusts the resolved OIDC provider.
    def _create_github_actions_deploy_role(self) -> iam.Role:
        """Role assumed by GitHub Actions via OIDC for deployments.

        Trust is restricted to the repository and common branches/environments.
        For stricter control, adjust the conditions below.
        """
        # Scope to your repository: owner/repo
        repo_owner = "keemgdeok"
        repo_name = "fin-forge-pipeline"

        principal = iam.OpenIdConnectPrincipal(
            self.github_oidc_provider,
            conditions={
                "StringEquals": {"token.actions.githubusercontent.com:aud": "sts.amazonaws.com"},
                "StringLike": {
                    "token.actions.githubusercontent.com:sub": [
                        f"repo:{repo_owner}/{repo_name}:ref:refs/heads/main",
                        f"repo:{repo_owner}/{repo_name}:ref:refs/heads/develop",
                        f"repo:{repo_owner}/{repo_name}:environment:staging",
                        f"repo:{repo_owner}/{repo_name}:environment:prod",
                        f"repo:{repo_owner}/{repo_name}:environment:dev",
                    ]
                },
            },
        )

        role = iam.Role(
            self,
            "GitHubActionsDeployRole",
            role_name=f"{self.env_name}-github-actions-deploy-role",
            assumed_by=principal,
        )

        # CDK deployments require broad permissions across the resources managed by the stacks.
        # Scope permissions to the data platform environment where possible to respect least privilege.
        deploy_policy = iam.Policy(
            self,
            "GitHubActionsDeployInlinePolicy",
            policy_name=f"{self.env_name}-github-actions-deploy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "cloudformation:*",
                        "s3:*",
                        "lambda:*",
                        "logs:*",
                        "events:*",
                        "sns:*",
                        "sqs:*",
                        "states:*",
                        "glue:*",
                        "kms:*",
                        "cloudwatch:*",
                        "iam:ListOpenIDConnectProviders",
                    ],
                    resources=["*"],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "iam:CreateRole",
                        "iam:TagRole",
                        "iam:UntagRole",
                        "iam:UpdateRole",
                        "iam:UpdateAssumeRolePolicy",
                    ],
                    conditions={
                        "StringLike": {
                            "iam:RoleName": [
                                f"DataPlatform-{self.env_name}-*",
                                f"{self.env_name}-*",
                            ]
                        }
                    },
                    resources=["*"],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "iam:DeleteRole",
                        "iam:GetRole",
                        "iam:GetRolePolicy",
                        "iam:ListRolePolicies",
                        "iam:AttachRolePolicy",
                        "iam:DetachRolePolicy",
                        "iam:PutRolePolicy",
                        "iam:DeleteRolePolicy",
                    ],
                    resources=[
                        f"arn:aws:iam::{self.account}:role/DataPlatform-{self.env_name}-*",
                        f"arn:aws:iam::{self.account}:role/{self.env_name}-*",
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "iam:CreatePolicy",
                        "iam:DeletePolicy",
                        "iam:CreatePolicyVersion",
                        "iam:DeletePolicyVersion",
                    ],
                    conditions={"StringLike": {"iam:PolicyName": [f"DataPlatform-{self.env_name}-*"]}},
                    resources=["*"],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["iam:PassRole"],
                    resources=[
                        self.lambda_execution_role.role_arn,
                        self.glue_execution_role.role_arn,
                        self.step_functions_execution_role.role_arn,
                        f"arn:aws:iam::{self.account}:role/DataPlatform-{self.env_name}-*",
                        f"arn:aws:iam::{self.account}:role/{self.env_name}-*",
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["sts:AssumeRole", "sts:GetCallerIdentity"],
                    resources=["*"],
                ),
            ],
        )
        role.attach_inline_policy(deploy_policy)
        return role
