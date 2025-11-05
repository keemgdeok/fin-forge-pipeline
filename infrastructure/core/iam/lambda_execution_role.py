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

        raw_bucket_name = raw_bucket.bucket_name
        curated_bucket_name = curated_bucket.bucket_name
        artifacts_bucket_name = artifacts_bucket.bucket_name

        additional_patterns = iam_utils.config_string_list(config, "lambda_additional_s3_patterns", default=())

        schema_object_arns: list[str] = []
        curated_schema_object_arns: list[str] = []
        for trigger in list(config.get("processing_triggers", []) or []):
            domain = str(trigger.get("domain", "")).strip()
            table = str(trigger.get("table_name", "")).strip()
            if not domain or not table:
                continue
            schema_object_arns.append(f"arn:aws:s3:::{artifacts_bucket_name}/{domain}/{table}/_schema/*")
            curated_schema_object_arns.append(f"arn:aws:s3:::{curated_bucket_name}/{domain}/{table}/_schema/*")

        data_bucket_names = [raw_bucket_name, curated_bucket_name, artifacts_bucket_name]
        list_bucket_resources = iam_utils.dedupe(iam_utils.bucket_arn(name) for name in data_bucket_names)
        object_resources = iam_utils.dedupe(iam_utils.bucket_objects_arn(name) for name in data_bucket_names)

        lambda_s3_object_resources = iam_utils.dedupe(
            list(object_resources) + schema_object_arns + curated_schema_object_arns + additional_patterns
        )

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
                actions=["s3:ListBucket"],
                resources=list_bucket_resources,
            ),
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                resources=lambda_s3_object_resources,
            ),
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:GetObject", "s3:GetObjectVersion"],
                resources=[asset_objects_arn],
            ),
        ]

        sns_topic_arn = f"arn:aws:sns:{region}:{account}:{env_name}-data-platform-alerts"

        # table_stream_arn = getattr(batch_tracker_table, "table_stream_arn", None)

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
                # "DynamoDbBatchTrackerAccess": iam.PolicyDocument(
                #     statements=[
                #         iam.PolicyStatement(
                #             effect=iam.Effect.ALLOW,
                #             actions=[
                #                 "dynamodb:BatchGetItem",
                #                 "dynamodb:BatchWriteItem",
                #                 "dynamodb:ConditionCheckItem",
                #                 "dynamodb:DeleteItem",
                #                 "dynamodb:DescribeTable",
                #                 "dynamodb:GetItem",
                #                 "dynamodb:PutItem",
                #                 "dynamodb:Query",
                #                 "dynamodb:Scan",
                #                 "dynamodb:UpdateItem",
                #             ],
                #             resources=[batch_tracker_table.table_arn],
                #         ),
                #         *(
                #             [
                #                 iam.PolicyStatement(
                #                     effect=iam.Effect.ALLOW,
                #                     actions=[
                #                         "dynamodb:DescribeStream",
                #                         "dynamodb:GetRecords",
                #                         "dynamodb:GetShardIterator",
                #                     ],
                #                     resources=[table_stream_arn],
                #                 )
                #             ]
                #             if table_stream_arn
                #             else []
                #         ),
                #         iam.PolicyStatement(
                #             effect=iam.Effect.ALLOW,
                #             actions=["dynamodb:ListStreams"],
                #             resources=["*"],
                #         ),
                #     ]
                # ),
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

    @property
    def role(self) -> iam.Role:
        """Return the created IAM role."""
        return self._role
