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
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.environment = environment
        self.config = config

        # Core S3 buckets using existing construct
        self.data_lake = DataLakeConstruct(
            self, "DataLake",
            environment=environment,
            config=config,
        )
        self.raw_bucket = self.data_lake.raw_bucket
        self.curated_bucket = self.data_lake.curated_bucket
        
        # Additional artifacts bucket
        self.artifacts_bucket = self._create_artifacts_bucket()

        # Metadata and state management
        self.pipeline_state_table = self._create_pipeline_state_table()
        self.job_metadata_table = self._create_job_metadata_table()

        self._create_outputs()


    def _create_artifacts_bucket(self) -> s3.Bucket:
        """Create artifacts bucket for scripts, configs, etc."""
        return s3.Bucket(
            self,
            "ArtifactsBucket",
            bucket_name=f"{self.environment}-data-platform-artifacts-{self.account}",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.DESTROY,
        )

    def _create_pipeline_state_table(self) -> dynamodb.Table:
        """Create DynamoDB table for pipeline state management."""
        return dynamodb.Table(
            self,
            "PipelineStateTable",
            table_name=f"{self.environment}-pipeline-state",
            partition_key=dynamodb.Attribute(
                name="pipeline_id",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="execution_id",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="ttl",
            removal_policy=RemovalPolicy.DESTROY,
        )

    def _create_job_metadata_table(self) -> dynamodb.Table:
        """Create DynamoDB table for job metadata and lineage."""
        return dynamodb.Table(
            self,
            "JobMetadataTable",
            table_name=f"{self.environment}-job-metadata",
            partition_key=dynamodb.Attribute(
                name="job_id",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="timestamp",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

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
            "PipelineStateTableName",
            value=self.pipeline_state_table.table_name,
            description="Pipeline state DynamoDB table name",
        )

        CfnOutput(
            self,
            "JobMetadataTableName", 
            value=self.job_metadata_table.table_name,
            description="Job metadata DynamoDB table name",
        )