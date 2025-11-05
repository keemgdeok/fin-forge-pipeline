"""Shared storage infrastructure for serverless data platform."""

from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    RemovalPolicy,
    CfnOutput,
)
from constructs import Construct
from infrastructure.constructs.data_lake_construct import DataLakeConstruct


class SharedStorageStack(Stack):
    """Central storage stack for platform-wide data storage needs."""

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

        # Core S3 buckets using existing construct
        self.data_lake = DataLakeConstruct(
            self,
            "DataLake",
            environment=environment,
            config=config,
        )
        self.raw_bucket = self.data_lake.raw_bucket
        self.curated_bucket = self.data_lake.curated_bucket

        # Additional artifacts bucket
        self.artifacts_bucket = self._create_artifacts_bucket()
        self.batch_tracker_table = self._create_batch_tracker_table()

        self._create_outputs()

    def _create_artifacts_bucket(self) -> s3.Bucket:
        """Create artifacts bucket for scripts, configs, etc."""
        # Align removal policy and auto delete with environment config
        cfg_policy = (self.config.get("removal_policy", "destroy") or "destroy").lower()
        if cfg_policy == "retain":
            removal_policy = RemovalPolicy.RETAIN
        elif cfg_policy == "destroy":
            removal_policy = RemovalPolicy.DESTROY
        else:
            removal_policy = RemovalPolicy.RETAIN if self.env_name == "prod" else RemovalPolicy.DESTROY
        auto_delete = bool(self.config.get("auto_delete_objects", False))

        return s3.Bucket(
            self,
            "ArtifactsBucket",
            bucket_name=f"{self.env_name}-data-platform-artifacts-{self.account}",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=removal_policy,
            auto_delete_objects=auto_delete,
        )

    def _create_batch_tracker_table(self) -> dynamodb.Table:
        """Provision the shared DynamoDB batch tracker table."""
        table_name = self._resolve_batch_tracker_table_name()
        orchestration_mode = str(self.config.get("processing_orchestration_mode", "manual")).lower()
        enable_streams = orchestration_mode == "dynamodb_stream"
        enable_pitr = bool(self.config.get("enable_point_in_time_recovery", False))

        table = dynamodb.Table(
            self,
            "BatchTrackerTable",
            table_name=table_name,
            partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="ttl",
            stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES if enable_streams else None,
            point_in_time_recovery=enable_pitr,
        )

        cfg_policy = str(self.config.get("removal_policy", "retain") or "retain").lower()
        if cfg_policy == "destroy":
            table.apply_removal_policy(RemovalPolicy.DESTROY)
        else:
            table.apply_removal_policy(RemovalPolicy.RETAIN)

        return table

    def _resolve_batch_tracker_table_name(self) -> str:
        """Return the configured batch tracker table name."""
        raw_name = str(self.config.get("batch_tracker_table_name") or "").strip()
        if raw_name:
            return raw_name
        return f"{self.env_name}-daily-prices-batch-tracker"

    def _create_outputs(self) -> None:
        """Create CloudFormation outputs."""
        CfnOutput(
            self,
            "RawBucketName",
            value=self.raw_bucket.bucket_name,
            description="Raw data S3 bucket name",
        )

        CfnOutput(
            self,
            "CuratedBucketName",
            value=self.curated_bucket.bucket_name,
            description="Curated data S3 bucket name",
        )

        CfnOutput(
            self,
            "ArtifactsBucketName",
            value=self.artifacts_bucket.bucket_name,
            description="Artifacts S3 bucket name",
        )

        CfnOutput(
            self,
            "BatchTrackerTableName",
            value=self.batch_tracker_table.table_name,
            description="Batch tracker DynamoDB table name",
        )
