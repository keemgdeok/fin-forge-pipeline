"""Data catalog and governance stack for data platform."""
from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_athena as athena,
    aws_lakeformation as lf,
    CfnOutput,
)
from constructs import Construct


class DataCatalogStack(Stack):
    """Central data catalog and governance stack."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: str,
        config: dict,
        shared_storage_stack,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.environment = environment
        self.config = config
        self.shared_storage = shared_storage_stack

        # Glue Data Catalog database
        self.glue_database = self._create_glue_database()
        
        # Athena workgroup for queries
        self.athena_workgroup = self._create_athena_workgroup()
        
        # Data crawlers for automatic schema discovery
        self.crawlers = self._create_data_crawlers()

        self._create_outputs()

    def _create_glue_database(self) -> glue.CfnDatabase:
        """Create Glue Data Catalog database."""
        return glue.CfnDatabase(
            self,
            "DataPlatformDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=f"{self.environment}_data_platform",
                description="Central data catalog for data platform",
                parameters={
                    "environment": self.environment,
                    "created_by": "cdk",
                    "classification": "data-platform",
                },
            ),
        )

    def _create_athena_workgroup(self) -> athena.CfnWorkGroup:
        """Create Athena workgroup for data queries."""
        return athena.CfnWorkGroup(
            self,
            "DataPlatformWorkgroup",
            name=f"{self.environment}-data-platform-workgroup",
            description="Athena workgroup for data platform queries",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{self.shared_storage.artifacts_bucket.bucket_name}/athena-results/",
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_S3",
                    ),
                ),
                enforce_work_group_configuration=True,
                publish_cloud_watch_metrics=True,
                bytes_scanned_cutoff_per_query=1000000000,  # 1GB limit
            ),
        )

    def _create_data_crawlers(self) -> dict:
        """Create Glue crawlers for automatic schema discovery."""
        crawlers = {}

        # Raw data crawler
        crawlers["raw_data"] = glue.CfnCrawler(
            self,
            "RawDataCrawler",
            name=f"{self.environment}-raw-data-crawler",
            role=f"arn:aws:iam::{self.account}:role/service-role/AWSGlueServiceRole-DataCrawler",
            database_name=self.glue_database.ref,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{self.shared_storage.raw_bucket.bucket_name}/",
                    )
                ]
            ),
            schedule=glue.CfnCrawler.ScheduleProperty(
                schedule_expression="cron(0 6 * * ? *)",  # Daily at 6 AM
            ),
            configuration='{"Version":1.0,"CrawlerOutput":{"Partitions":{"AddOrUpdateBehavior":"InheritFromTable"}},"Grouping":{"TableGroupingPolicy":"CombineCompatibleSchemas"}}',
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="UPDATE_IN_DATABASE",
                delete_behavior="LOG",
            ),
            table_prefix="raw_",
        )

        # Curated data crawler
        crawlers["curated_data"] = glue.CfnCrawler(
            self,
            "CuratedDataCrawler", 
            name=f"{self.environment}-curated-data-crawler",
            role=f"arn:aws:iam::{self.account}:role/service-role/AWSGlueServiceRole-DataCrawler",
            database_name=self.glue_database.ref,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{self.shared_storage.curated_bucket.bucket_name}/",
                    )
                ]
            ),
            schedule=glue.CfnCrawler.ScheduleProperty(
                schedule_expression="cron(30 6 * * ? *)",  # Daily at 6:30 AM
            ),
            configuration='{"Version":1.0,"CrawlerOutput":{"Partitions":{"AddOrUpdateBehavior":"InheritFromTable"}},"Grouping":{"TableGroupingPolicy":"CombineCompatibleSchemas"}}',
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="UPDATE_IN_DATABASE", 
                delete_behavior="LOG",
            ),
            table_prefix="curated_",
        )

        return crawlers

    def _create_outputs(self) -> None:
        """Create CloudFormation outputs."""
        CfnOutput(
            self,
            "GlueDatabaseName",
            value=self.glue_database.ref,
            description="Glue Data Catalog database name",
        )

        CfnOutput(
            self,
            "AthenaWorkgroupName",
            value=self.athena_workgroup.name,
            description="Athena workgroup name for queries",
        )

        CfnOutput(
            self,
            "RawDataCrawlerName",
            value=self.crawlers["raw_data"].name,
            description="Raw data crawler name",
        )

        CfnOutput(
            self,
            "CuratedDataCrawlerName", 
            value=self.crawlers["curated_data"].name,
            description="Curated data crawler name",
        )