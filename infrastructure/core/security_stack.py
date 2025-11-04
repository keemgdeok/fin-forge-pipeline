"""Security foundation stack for serverless data platform."""

import os
from typing import Optional

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
        self.step_functions_execution_role = self._create_step_functions_execution_role()

        # GitHub Actions OIDC provider and deploy role
        # This enables CI/CD via GitHub Actions without long-lived keys.
        self.github_oidc_provider = self._create_github_oidc_provider()
        self.github_actions_deploy_role = self._create_github_actions_deploy_role()

        self._create_outputs()

    def _create_lambda_execution_role(self) -> iam.Role:
        """Create base Lambda execution role."""
        # Restrict S3 and Glue access to known resources
        raw_bucket_name = f"data-pipeline-raw-{self.env_name}-{self.account}"
        curated_bucket_name = f"data-pipeline-curated-{self.env_name}-{self.account}"
        artifacts_bucket_name = f"{self.env_name}-data-platform-artifacts-{self.account}"

        s3_bucket_arns = [
            f"arn:aws:s3:::{raw_bucket_name}",
            f"arn:aws:s3:::{raw_bucket_name}/*",
            f"arn:aws:s3:::{curated_bucket_name}",
            f"arn:aws:s3:::{curated_bucket_name}/*",
            f"arn:aws:s3:::{artifacts_bucket_name}",
            f"arn:aws:s3:::{artifacts_bucket_name}/*",
        ]

        # Build minimal schema prefix resources for Preflight/SchemaCheck
        schema_object_arns: list[str] = []
        curated_schema_object_arns: list[str] = []
        for t in list(self.config.get("processing_triggers", [])):
            d = str(t.get("domain", "")).strip()
            tbl = str(t.get("table_name", "")).strip()
            if not d or not tbl:
                continue
            schema_object_arns.append(f"arn:aws:s3:::{artifacts_bucket_name}/{d}/{tbl}/_schema/*")
            curated_schema_object_arns.append(f"arn:aws:s3:::{curated_bucket_name}/{d}/{tbl}/_schema/*")

        glue_job_arn = f"arn:aws:glue:{self.region}:{self.account}:job/{self.env_name}-daily-prices-data-etl"
        ingestion_queue_arn = f"arn:aws:sqs:{self.region}:{self.account}:{self.env_name}-ingestion-queue"
        load_queue_arn = f"arn:aws:sqs:{self.region}:{self.account}:{self.env_name}-*-load-queue"
        batch_tracker_table_name = str(
            self.config.get("batch_tracker_table_name") or f"{self.env_name}-daily-prices-batch-tracker"
        )
        batch_tracker_table_arn = self.format_arn(
            service="dynamodb",
            resource="table",
            resource_name=batch_tracker_table_name,
        )
        batch_tracker_stream_arn = f"{batch_tracker_table_arn}/stream/*"
        state_machine_arns = [
            f"arn:aws:states:{self.region}:{self.account}:stateMachine:{self.env_name}-{str(name).strip()}"
            for name in self.config.get("monitored_state_machines", [])
            if str(name).strip()
        ]
        if not state_machine_arns:
            state_machine_arns = [
                f"arn:aws:states:{self.region}:{self.account}:stateMachine:{self.env_name}-daily-prices-data-processing"
            ]

        role = iam.Role(
            self,
            "LambdaExecutionRole",
            role_name=f"{self.env_name}-data-platform-lambda-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
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
                            resources=s3_bucket_arns,
                        ),
                        # Minimal schema path access for artifacts/curated
                        *(
                            [
                                iam.PolicyStatement(
                                    effect=iam.Effect.ALLOW,
                                    actions=["s3:GetObject", "s3:PutObject"],
                                    resources=schema_object_arns + curated_schema_object_arns,
                                )
                            ]
                            if (schema_object_arns or curated_schema_object_arns)
                            else []
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
                            resources=[glue_job_arn],
                        ),
                    ]
                ),
                "StepFunctionsStartExecution": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["states:StartExecution"],
                            resources=state_machine_arns,
                        )
                    ]
                ),
                "SnsPublishAlerts": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["sns:Publish"],
                            resources=[
                                f"arn:aws:sns:{self.region}:{self.account}:{self.env_name}-data-platform-alerts"
                            ],
                        )
                    ]
                ),
                "SqsConsumeAccess": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "sqs:ChangeMessageVisibility",
                                "sqs:ChangeMessageVisibilityBatch",
                                "sqs:DeleteMessage",
                                "sqs:DeleteMessageBatch",
                                "sqs:GetQueueAttributes",
                                "sqs:GetQueueUrl",
                                "sqs:ReceiveMessage",
                            ],
                            resources=[ingestion_queue_arn],
                        )
                    ]
                ),
                "SqsSendMessage": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["sqs:SendMessage", "sqs:SendMessageBatch"],
                            resources=[ingestion_queue_arn, load_queue_arn],
                        )
                    ]
                ),
                "DynamoDbBatchTrackerAccess": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "dynamodb:BatchGetItem",
                                "dynamodb:BatchWriteItem",
                                "dynamodb:ConditionCheckItem",
                                "dynamodb:DeleteItem",
                                "dynamodb:DescribeTable",
                                "dynamodb:GetItem",
                                "dynamodb:PutItem",
                                "dynamodb:Query",
                                "dynamodb:Scan",
                                "dynamodb:UpdateItem",
                            ],
                            resources=[batch_tracker_table_arn],
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "dynamodb:DescribeStream",
                                "dynamodb:GetRecords",
                                "dynamodb:GetShardIterator",
                            ],
                            resources=[batch_tracker_stream_arn],
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["dynamodb:ListStreams"],
                            resources=["*"],
                        ),
                    ]
                ),
                "CloudWatchPutMetric": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["cloudwatch:PutMetricData"],
                            resources=["*"],
                        )
                    ]
                ),
                "SesSendEmail": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["ses:SendEmail", "ses:SendRawEmail"],
                            resources=["*"],
                            conditions={
                                "StringEquals": {
                                    # Limit to current region requests at minimum
                                    "aws:RequestedRegion": self.region,
                                    **(
                                        {"ses:FromAddress": str(self.config.get("notification_source_email"))}
                                        if self.config.get("notification_source_email")
                                        else {}
                                    ),
                                }
                            },
                        )
                    ]
                ),
            },
        )
        return role

    def _create_glue_execution_role(self) -> iam.Role:
        """Create Glue job execution role."""
        raw_bucket_name = f"data-pipeline-raw-{self.env_name}-{self.account}"
        curated_bucket_name = f"data-pipeline-curated-{self.env_name}-{self.account}"
        artifacts_bucket_name = f"{self.env_name}-data-platform-artifacts-{self.account}"

        s3_bucket_arns = [
            f"arn:aws:s3:::{raw_bucket_name}",
            f"arn:aws:s3:::{raw_bucket_name}/*",
            f"arn:aws:s3:::{curated_bucket_name}",
            f"arn:aws:s3:::{curated_bucket_name}/*",
            f"arn:aws:s3:::{artifacts_bucket_name}",
            f"arn:aws:s3:::{artifacts_bucket_name}/*",
        ]

        return iam.Role(
            self,
            "GlueExecutionRole",
            role_name=f"{self.env_name}-data-platform-glue-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
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
                            resources=s3_bucket_arns,
                        ),
                    ]
                ),
            },
        )

    def _create_step_functions_execution_role(self) -> iam.Role:
        """Create Step Functions execution role."""
        preflight_fn_name = f"{self.env_name}-daily-prices-data-preflight"
        schema_decider_fn_name = f"{self.env_name}-schema-change-decider"
        glue_job_arn = f"arn:aws:glue:{self.region}:{self.account}:job/{self.env_name}-daily-prices-data-etl"
        crawler_arn = f"arn:aws:glue:{self.region}:{self.account}:crawler/{self.env_name}-curated-data-crawler"

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
                            resources=[
                                f"arn:aws:lambda:{self.region}:{self.account}:function/{preflight_fn_name}",
                                f"arn:aws:lambda:{self.region}:{self.account}:function/{schema_decider_fn_name}",
                            ],
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
                            resources=[glue_job_arn],
                        ),
                    ]
                ),
                "GlueCrawlerManagement": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "glue:StartCrawler",
                                "glue:GetCrawler",
                            ],
                            resources=[crawler_arn],
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
