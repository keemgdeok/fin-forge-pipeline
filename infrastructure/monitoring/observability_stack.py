"""Unified observability stack for serverless data platform."""
from aws_cdk import (
    Stack,
    aws_cloudwatch as cloudwatch,
    aws_sns as sns,
    aws_cloudwatch_actions as cw_actions,
    aws_logs as logs,
    Duration,
    CfnOutput,
)
from constructs import Construct


class ObservabilityStack(Stack):
    """Central monitoring and observability for all data pipelines."""

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

        # SNS topics for alerting
        self.critical_alerts_topic = self._create_critical_alerts_topic()
        self.warning_alerts_topic = self._create_warning_alerts_topic()

        # CloudWatch dashboard
        self.platform_dashboard = self._create_platform_dashboard()

        # Platform-wide alarms
        self._create_platform_alarms()

        # Log groups
        self.log_groups = self._create_log_groups()

        self._create_outputs()

    def _create_critical_alerts_topic(self) -> sns.Topic:
        """Create SNS topic for critical alerts."""
        return sns.Topic(
            self,
            "CriticalAlertsTopic",
            topic_name=f"{self.environment}-data-platform-critical-alerts",
            display_name="Data Platform Critical Alerts",
        )

    def _create_warning_alerts_topic(self) -> sns.Topic:
        """Create SNS topic for warning alerts."""
        return sns.Topic(
            self,
            "WarningAlertsTopic", 
            topic_name=f"{self.environment}-data-platform-warning-alerts",
            display_name="Data Platform Warning Alerts",
        )

    def _create_platform_dashboard(self) -> cloudwatch.Dashboard:
        """Create unified CloudWatch dashboard."""
        dashboard = cloudwatch.Dashboard(
            self,
            "PlatformDashboard",
            dashboard_name=f"{self.environment}-data-platform-overview",
        )

        # Lambda metrics widget
        lambda_metrics_widget = cloudwatch.GraphWidget(
            title="Lambda Functions Overview",
            width=12,
            height=6,
            left=[
                cloudwatch.Metric(
                    namespace="AWS/Lambda",
                    metric_name="Invocations",
                    statistic="Sum",
                ),
                cloudwatch.Metric(
                    namespace="AWS/Lambda", 
                    metric_name="Errors",
                    statistic="Sum",
                ),
                cloudwatch.Metric(
                    namespace="AWS/Lambda",
                    metric_name="Duration",
                    statistic="Average",
                ),
            ],
        )

        # Step Functions metrics widget  
        step_functions_widget = cloudwatch.GraphWidget(
            title="Step Functions Overview",
            width=12,
            height=6,
            left=[
                cloudwatch.Metric(
                    namespace="AWS/States",
                    metric_name="ExecutionsStarted",
                    statistic="Sum",
                ),
                cloudwatch.Metric(
                    namespace="AWS/States",
                    metric_name="ExecutionsFailed", 
                    statistic="Sum",
                ),
                cloudwatch.Metric(
                    namespace="AWS/States",
                    metric_name="ExecutionsSucceeded",
                    statistic="Sum", 
                ),
            ],
        )

        # Glue jobs metrics widget
        glue_widget = cloudwatch.GraphWidget(
            title="Glue Jobs Overview",
            width=12,
            height=6,
            left=[
                cloudwatch.Metric(
                    namespace="AWS/Glue",
                    metric_name="glue.driver.aggregate.numCompletedTasks",
                    statistic="Sum",
                ),
                cloudwatch.Metric(
                    namespace="AWS/Glue", 
                    metric_name="glue.driver.aggregate.numFailedTasks",
                    statistic="Sum",
                ),
            ],
        )

        # S3 metrics widget
        s3_widget = cloudwatch.GraphWidget(
            title="S3 Storage Overview", 
            width=12,
            height=6,
            left=[
                cloudwatch.Metric(
                    namespace="AWS/S3",
                    metric_name="NumberOfObjects",
                    statistic="Average",
                ),
                cloudwatch.Metric(
                    namespace="AWS/S3",
                    metric_name="BucketSizeBytes",
                    statistic="Average",
                ),
            ],
        )

        dashboard.add_widgets(
            lambda_metrics_widget,
            step_functions_widget,
            glue_widget,
            s3_widget,
        )

        return dashboard

    def _create_platform_alarms(self) -> None:
        """Create platform-wide CloudWatch alarms."""
        # High Lambda error rate alarm
        lambda_error_alarm = cloudwatch.Alarm(
            self,
            "HighLambdaErrorRate",
            alarm_name=f"{self.environment}-high-lambda-error-rate",
            alarm_description="High error rate across Lambda functions",
            metric=cloudwatch.Metric(
                namespace="AWS/Lambda",
                metric_name="Errors",
                statistic="Sum",
            ),
            threshold=10,
            evaluation_periods=2,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
        )
        
        lambda_error_alarm.add_alarm_action(
            cw_actions.SnsAction(self.critical_alerts_topic)
        )

        # Step Functions failure alarm
        sf_failure_alarm = cloudwatch.Alarm(
            self,
            "StepFunctionsFailures",
            alarm_name=f"{self.environment}-step-functions-failures",
            alarm_description="Step Functions executions failing",
            metric=cloudwatch.Metric(
                namespace="AWS/States", 
                metric_name="ExecutionsFailed",
                statistic="Sum",
            ),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        )

        sf_failure_alarm.add_alarm_action(
            cw_actions.SnsAction(self.critical_alerts_topic)
        )

    def _create_log_groups(self) -> dict:
        """Create standardized log groups for platform components."""
        log_groups = {}
        
        # Platform-wide log groups
        components = ["lambda", "glue", "stepfunctions", "pipeline-orchestration"]
        
        for component in components:
            log_groups[component] = logs.LogGroup(
                self,
                f"{component.title()}LogGroup",
                log_group_name=f"/aws/{component}/{self.environment}-data-platform",
                retention=logs.RetentionDays.ONE_MONTH if self.environment != "prod" else logs.RetentionDays.THREE_MONTHS,
            )

        return log_groups

    def _create_outputs(self) -> None:
        """Create CloudFormation outputs."""
        CfnOutput(
            self,
            "CriticalAlertsTopicArn", 
            value=self.critical_alerts_topic.topic_arn,
            description="Critical alerts SNS topic ARN",
        )

        CfnOutput(
            self,
            "WarningAlertsTopicArn",
            value=self.warning_alerts_topic.topic_arn,
            description="Warning alerts SNS topic ARN", 
        )

        CfnOutput(
            self,
            "PlatformDashboardUrl",
            value=f"https://console.aws.amazon.com/cloudwatch/home?region={self.region}#dashboards:name={self.platform_dashboard.dashboard_name}",
            description="Platform dashboard URL",
        )