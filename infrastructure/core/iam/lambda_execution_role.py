"""Construct providing the shared Lambda execution role."""

from __future__ import annotations

from aws_cdk import Stack, aws_iam as iam, aws_s3 as s3, aws_dynamodb as dynamodb
from constructs import Construct

from infrastructure.config.types import EnvironmentConfig
from infrastructure.core.iam import utils as iam_utils


class LambdaExecutionRoleConstruct(Construct):
    """Provision the Lambda execution role with least-privilege defaults."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        env_name: str,
        config: EnvironmentConfig,
        raw_bucket: s3.IBucket,
        curated_bucket: s3.IBucket,
        artifacts_bucket: s3.IBucket,
        batch_tracker_table: dynamodb.ITable,
    ) -> None:
        super().__init__(scope, construct_id)
        stack = Stack.of(self)
        account = stack.account
        region = stack.region

        asset_bucket_name = iam_utils.bootstrap_asset_bucket_name(stack)

        additional_patterns = iam_utils.config_string_list(config, "lambda_additional_s3_patterns", default=())

        schema_object_arns: list[str] = []
        curated_schema_object_arns: list[str] = []
        for trigger in list(config.get("processing_triggers", []) or []):
            domain = str(trigger.get("domain", "")).strip()
            table = str(trigger.get("table_name", "")).strip()
            if not domain or not table:
                continue
            schema_object_arns.append(f"arn:aws:s3:::{artifacts_bucket.bucket_name}/{domain}/{table}/_schema/*")
            curated_schema_object_arns.append(f"arn:aws:s3:::{curated_bucket.bucket_name}/{domain}/{table}/_schema/*")

        extra_object_resources = iam_utils.dedupe(schema_object_arns + curated_schema_object_arns)

        glue_job_arn = f"arn:aws:glue:{region}:{account}:job/{env_name}-daily-prices-data-etl"
        ingestion_queue_arn = f"arn:aws:sqs:{region}:{account}:{env_name}-ingestion-queue"
        load_queue_arn = f"arn:aws:sqs:{region}:{account}:{env_name}-*-load-queue"

        monitored_state_machines = [
            f"arn:aws:states:{region}:{account}:stateMachine:{env_name}-{str(name).strip()}"
            for name in config.get("monitored_state_machines", [])
            if str(name).strip()
        ]
        if not monitored_state_machines:
            monitored_state_machines = [
                f"arn:aws:states:{region}:{account}:stateMachine:{env_name}-daily-prices-data-processing"
            ]

        asset_objects_arn = iam_utils.bucket_objects_arn(asset_bucket_name)

        s3_statements: list[iam.PolicyStatement] = [
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:GetObject", "s3:GetObjectVersion"],
                resources=[asset_objects_arn],
            ),
        ]

        if extra_object_resources:
            s3_statements.append(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                    resources=extra_object_resources,
                )
            )

        sns_topic_arn = f"arn:aws:sns:{region}:{account}:{env_name}-data-platform-alerts"

        self._role = iam.Role(
            self,
            "Role",
            role_name=f"{env_name}-data-platform-lambda-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
            ],
            inline_policies={
                "S3Access": iam.PolicyDocument(statements=s3_statements),
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
                            resources=monitored_state_machines,
                        )
                    ]
                ),
                "SnsPublishAlerts": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["sns:Publish"],
                            resources=[sns_topic_arn],
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
                                    "aws:RequestedRegion": region,
                                    **(
                                        {"ses:FromAddress": str(config.get("notification_source_email"))}
                                        if config.get("notification_source_email")
                                        else {}
                                    ),
                                }
                            },
                        )
                    ]
                ),
            },
        )

        # Grant resource access via constructs to respect future refactors
        raw_bucket.grant_read_write(self._role)
        curated_bucket.grant_read_write(self._role)
        artifacts_bucket.grant_read_write(self._role)

        batch_tracker_table.grant_read_write_data(self._role)
        batch_tracker_table.grant_stream_read(self._role)

        # DynamoDB ListStreams is not covered by grant
        self._role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["dynamodb:ListStreams"],
                resources=["*"],
            )
        )

        # Ensure additional bucket prefixes remain accessible when configured
        if additional_patterns:
            self._role.add_to_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                    resources=additional_patterns,
                )
            )

    @property
    def role(self) -> iam.Role:
        """Return the created IAM role."""
        return self._role
