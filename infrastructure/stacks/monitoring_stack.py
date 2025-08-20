"""Monitoring stack for CloudWatch dashboards and alarms."""
from aws_cdk import (
    Stack,
    aws_cloudwatch as cloudwatch,
)
from constructs import Construct


class MonitoringStack(Stack):
    """Stack containing monitoring and alerting for the data pipeline."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: str,
        config: dict,
        compute_stack,
        orchestration_stack,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.environment = environment
        self.config = config
        self.compute_stack = compute_stack
        self.orchestration_stack = orchestration_stack

        # Create monitoring components
        self._create_dashboard()
        self._create_alarms()

    def _create_dashboard(self) -> None:
        """Create CloudWatch dashboard for monitoring."""
        self.dashboard = cloudwatch.Dashboard(
            self, "DataPipelineDashboard",
            dashboard_name=f"DataPipeline-{self.environment}",
        )

        # Add Lambda function metrics
        if hasattr(self.compute_stack, 'lambda_functions'):
            lambda_widgets = []
            for func_name, func in self.compute_stack.lambda_functions.items():
                lambda_widgets.extend([
                    cloudwatch.GraphWidget(
                        title=f"{func_name} - Duration",
                        left=[func.metric_duration()],
                    ),
                    cloudwatch.GraphWidget(
                        title=f"{func_name} - Errors",
                        left=[func.metric_errors()],
                    ),
                ])

            if lambda_widgets:
                self.dashboard.add_widgets(*lambda_widgets)

        # Add Step Functions metrics
        if hasattr(self.orchestration_stack, 'step_function'):
            sf_widgets = [
                cloudwatch.GraphWidget(
                    title="Step Function Executions",
                    left=[
                        self.orchestration_stack.step_function.metric_started(),
                        self.orchestration_stack.step_function.metric_succeeded(),
                        self.orchestration_stack.step_function.metric_failed(),
                    ],
                ),
            ]
            self.dashboard.add_widgets(*sf_widgets)

    def _create_alarms(self) -> None:
        """Create CloudWatch alarms for critical metrics."""
        # Lambda function error alarms
        if hasattr(self.compute_stack, 'lambda_functions'):
            for func_name, func in self.compute_stack.lambda_functions.items():
                cloudwatch.Alarm(
                    self, f"{func_name}ErrorAlarm",
                    alarm_name=f"DataPipeline-{self.environment}-{func_name}-Errors",
                    metric=func.metric_errors(period=cloudwatch.Duration.minutes(5)),
                    threshold=1,
                    evaluation_periods=1,
                    comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
                )

        # Step Functions failure alarm
        if hasattr(self.orchestration_stack, 'step_function'):
            cloudwatch.Alarm(
                self, "StepFunctionFailureAlarm",
                alarm_name=f"DataPipeline-{self.environment}-StepFunction-Failures",
                metric=self.orchestration_stack.step_function.metric_failed(
                    period=cloudwatch.Duration.minutes(5)
                ),
                threshold=1,
                evaluation_periods=1,
                comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            )