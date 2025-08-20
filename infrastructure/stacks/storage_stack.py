"""Storage stack for S3 buckets and related resources."""
from aws_cdk import (
    Stack,
    aws_s3 as s3,
    RemovalPolicy,
    Duration,
    CfnOutput,
)
from constructs import Construct
from ..constructs.data_lake_construct import DataLakeConstruct


class StorageStack(Stack):
    """Stack containing S3 buckets for data storage."""

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

        # Data Lake Construct
        self.data_lake = DataLakeConstruct(
            self, "DataLake",
            environment=environment,
            config=config
        )

        # Expose buckets for other stacks
        self.raw_bucket = self.data_lake.raw_bucket
        self.curated_bucket = self.data_lake.curated_bucket

        # CloudFormation Outputs
        self._create_outputs()

    def _create_outputs(self) -> None:
        """Create CloudFormation outputs for the storage resources."""
        CfnOutput(
            self, "RawBucketName",
            value=self.raw_bucket.bucket_name,
            description="Name of the raw data S3 bucket"
        )

        CfnOutput(
            self, "RawBucketArn",
            value=self.raw_bucket.bucket_arn,
            description="ARN of the raw data S3 bucket"
        )

        CfnOutput(
            self, "CuratedBucketName",
            value=self.curated_bucket.bucket_name,
            description="Name of the curated data S3 bucket"
        )

        CfnOutput(
            self, "CuratedBucketArn",
            value=self.curated_bucket.bucket_arn,
            description="ARN of the curated data S3 bucket"
        )