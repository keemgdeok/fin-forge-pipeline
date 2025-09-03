"""Data Lake construct for S3 buckets with proper configuration."""

from aws_cdk import (
    aws_s3 as s3,
    RemovalPolicy,
    Duration,
    Aws,
)
from constructs import Construct


class DataLakeConstruct(Construct):
    """Construct for creating a data lake with S3 buckets."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: str,
        config: dict,
    ) -> None:
        super().__init__(scope, construct_id)

        self.env_name = environment
        self.config = config

        # Create S3 buckets
        self._create_raw_bucket()
        self._create_curated_bucket()

    def _create_raw_bucket(self) -> None:
        """Create S3 bucket for raw data."""
        removal_policy = (
            RemovalPolicy.RETAIN
            if self.env_name == "prod"
            else RemovalPolicy.DESTROY
        )

        self.raw_bucket = s3.Bucket(
            self,
            "RawDataBucket",
            bucket_name=f"data-pipeline-raw-{self.env_name}-{Aws.ACCOUNT_ID}",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="DeleteOldVersions",
                    expired_object_delete_marker=True,
                    noncurrent_version_expiration=Duration.days(
                        self.config.get("s3_retention_days", 30)
                    ),
                ),
            ],
            removal_policy=removal_policy,
            auto_delete_objects=self.config.get("auto_delete_objects", False),
            enforce_ssl=True,
        )

    def _create_curated_bucket(self) -> None:
        """Create S3 bucket for curated/processed data."""
        removal_policy = (
            RemovalPolicy.RETAIN
            if self.env_name == "prod"
            else RemovalPolicy.DESTROY
        )

        self.curated_bucket = s3.Bucket(
            self,
            "CuratedDataBucket",
            bucket_name=f"data-pipeline-curated-{self.env_name}-{Aws.ACCOUNT_ID}",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="TransitionToIA",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after=Duration.days(30),
                        ),
                        s3.Transition(
                            storage_class=s3.StorageClass.GLACIER,
                            transition_after=Duration.days(90),
                        ),
                    ],
                ),
            ],
            removal_policy=removal_policy,
            auto_delete_objects=self.config.get("auto_delete_objects", False),
            enforce_ssl=True,
        )
