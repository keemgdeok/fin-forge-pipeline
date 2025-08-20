"""Analytics stack for Glue Data Catalog and Athena."""
from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_athena as athena,
    Aws,
)
from constructs import Construct


class AnalyticsStack(Stack):
    """Stack containing Glue Data Catalog and Athena resources."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: str,
        config: dict,
        raw_bucket,
        curated_bucket,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.environment = environment
        self.config = config
        self.raw_bucket = raw_bucket
        self.curated_bucket = curated_bucket

        # Create Glue Database
        self._create_glue_database()

        # Create Glue Crawler
        self._create_glue_crawler()

        # Create Athena Workgroup
        self._create_athena_workgroup()

    def _create_glue_database(self) -> None:
        """Create Glue Data Catalog database."""
        self.glue_database = glue.CfnDatabase(
            self, "DataCatalog",
            catalog_id=Aws.ACCOUNT_ID,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=f"data-pipeline-{self.environment}",
                description=f"Data pipeline catalog database for {self.environment}",
            ),
        )

    def _create_glue_crawler(self) -> None:
        """Create Glue Crawler for data discovery."""
        # Note: Crawler role would need to be created in security stack
        pass

    def _create_athena_workgroup(self) -> None:
        """Create Athena workgroup for query organization."""
        self.athena_workgroup = athena.CfnWorkGroup(
            self, "AthenaWorkgroup",
            name=f"data-pipeline-{self.environment}",
            description=f"Athena workgroup for {self.environment} data pipeline",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration=True,
                publish_cloud_watch_metrics=True,
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{self.curated_bucket.bucket_name}/athena-results/",
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_S3",
                    ),
                ),
            ),
        )