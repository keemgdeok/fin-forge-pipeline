"""Orchestration stack for Step Functions workflows."""
from aws_cdk import (
    Stack,
    aws_stepfunctions as stepfunctions,
    aws_stepfunctions_tasks as sfn_tasks,
    aws_logs as logs,
    Duration,
)
from constructs import Construct


class OrchestrationStack(Stack):
    """Stack containing Step Functions for workflow orchestration."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: str,
        config: dict,
        compute_stack,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.environment = environment
        self.config = config
        self.compute_stack = compute_stack

        # Create Step Functions workflow
        self._create_step_function_workflow()

    def _create_step_function_workflow(self) -> None:
        """Create the main data pipeline Step Functions workflow."""
        # Define Step Functions tasks using Lambda functions from compute stack
        data_ingestion_task = sfn_tasks.LambdaInvoke(
            self, "DataIngestionTask",
            lambda_function=self.compute_stack.lambda_functions.get("data_ingestion"),
            output_path="$.Payload",
        )

        glue_etl_task = sfn_tasks.LambdaInvoke(
            self, "GlueEtlTask",
            lambda_function=self.compute_stack.lambda_functions.get("glue_etl_trigger"),
            output_path="$.Payload",
        )

        data_processing_task = sfn_tasks.LambdaInvoke(
            self, "DataProcessingTask",
            lambda_function=self.compute_stack.lambda_functions.get("data_transformation"),
            output_path="$.Payload",
        )

        notification_task = sfn_tasks.LambdaInvoke(
            self, "NotificationTask",
            lambda_function=self.compute_stack.lambda_functions.get("notification_handler"),
            output_path="$.Payload",
        )

        error_handler_task = sfn_tasks.LambdaInvoke(
            self, "ErrorHandlerTask",
            lambda_function=self.compute_stack.lambda_functions.get("error_handler"),
            output_path="$.Payload",
        )

        # Define the workflow with error handling
        definition = data_ingestion_task.next(
            glue_etl_task
        ).next(
            data_processing_task
        ).next(
            notification_task
        ).add_catch(
            error_handler_task,
            errors=[stepfunctions.Errors.ALL],
            result_path="$.error"
        )

        # Create the Step Functions state machine
        self.step_function = stepfunctions.StateMachine(
            self, "DataPipelineWorkflow",
            definition=definition,
            timeout=Duration.hours(
                self.config.get("step_function_timeout_hours", 2)
            ),
            tracing_enabled=self.config.get("enable_xray_tracing", True),
        )