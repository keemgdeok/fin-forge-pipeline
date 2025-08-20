"""Main data pipeline stack that orchestrates all components."""
from aws_cdk import (
    Stack,
    CfnOutput,
)
from constructs import Construct


class DataPipelineStack(Stack):
    """Main orchestration stack for the data pipeline."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: str,
        config: dict,
        storage_stack,
        compute_stack,
        orchestration_stack,
        messaging_stack,
        analytics_stack,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.environment = environment
        self.config = config
        self.storage_stack = storage_stack
        self.compute_stack = compute_stack
        self.orchestration_stack = orchestration_stack
        self.messaging_stack = messaging_stack
        self.analytics_stack = analytics_stack

        # Create outputs for important resources
        self._create_outputs()

    def _create_outputs(self) -> None:
        """Create CloudFormation outputs for the data pipeline."""
        CfnOutput(
            self, "DataPipelineEnvironment",
            value=self.environment,
            description="Environment for this data pipeline deployment"
        )

        CfnOutput(
            self, "RawBucket",
            value=self.storage_stack.raw_bucket.bucket_name,
            description="Raw data S3 bucket name"
        )

        CfnOutput(
            self, "CuratedBucket", 
            value=self.storage_stack.curated_bucket.bucket_name,
            description="Curated data S3 bucket name"
        )

        if hasattr(self.orchestration_stack, 'step_function'):
            CfnOutput(
                self, "StepFunctionArn",
                value=self.orchestration_stack.step_function.state_machine_arn,
                description="Step Functions state machine ARN"
            )

        CfnOutput(
            self, "GlueDatabase",
            value=self.analytics_stack.glue_database.ref,
            description="Glue Data Catalog database name"
        )