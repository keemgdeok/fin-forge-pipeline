"""Unified observability stack for serverless data platform."""

from aws_cdk import (
    Stack,
    aws_cloudwatch as cloudwatch,
    aws_sns as sns,
    aws_cloudwatch_actions as cw_actions,
    aws_lambda as _lambda,
    aws_sqs as sqs,
    # aws_logs as logs,  # OPTIONAL: Only if custom log groups needed
    Duration,
    CfnOutput,
)
from constructs import Construct
from typing import Optional


class ObservabilityStack(Stack):
    """Central monitoring and observability for all data pipelines."""

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

        # SNS topic for alerting - single topic for small teams
        self.alerts_topic = self._create_alerts_topic()

        # CloudWatch dashboard
        self.platform_dashboard = self._create_platform_dashboard()

        # Essential alarms only
        self._create_essential_alarms()
        self._create_glue_job_alarms()
        self._create_state_machine_alarms()

        # OPTIONAL: Custom log groups (AWS creates them automatically)
        # self.log_groups = self._create_log_groups()

        self._create_outputs()

    def add_ingestion_pipeline_monitoring(
        self,
        *,
        pipeline_name: str,
        queue: sqs.IQueue,
        worker_function: _lambda.IFunction,
        depth_threshold: Optional[float] = None,
        age_threshold: Optional[float] = None,
    ) -> None:
        """Attach SQS/Lambda monitoring for an ingestion pipeline.

        Parameters
        ----------
        pipeline_name: str
            Friendly name used for alarm titles and dashboard widgets.
        queue: sqs.IQueue
            The ingestion queue to monitor.
        worker_function: aws_lambda.IFunction
            Worker Lambda processing the queue messages.
        depth_threshold: float, optional
            Override for queue depth alarm threshold (messages).
        age_threshold: float, optional
            Override for queue age alarm threshold (seconds).
        """

        def _sanitize(name: str) -> str:
            trimmed = "".join(ch for ch in name.title() if ch.isalnum())
            return trimmed or "Ingestion"

        identifier = _sanitize(pipeline_name)
        period = Duration.minutes(5)

        depth_metric = queue.metric_approximate_number_of_messages_visible(period=period, statistic="Average")
        age_metric = queue.metric_approximate_age_of_oldest_message(period=period)
        errors_metric = worker_function.metric_errors(period=period)
        throttles_metric = worker_function.metric_throttles(period=period)

        resolved_depth_threshold = (
            depth_threshold
            if depth_threshold is not None
            else float(self.config.get("alarm_queue_depth_threshold", 100.0))
        )
        resolved_age_threshold = (
            age_threshold if age_threshold is not None else float(self.config.get("alarm_queue_age_seconds", 300.0))
        )

        depth_alarm = cloudwatch.Alarm(
            self,
            f"{identifier}QueueDepthAlarm",
            alarm_name=f"{self.env_name}-{identifier}-queue-depth",
            alarm_description=f"{pipeline_name} ingestion queue depth high",
            metric=depth_metric,
            threshold=resolved_depth_threshold,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        )
        depth_alarm.add_alarm_action(cw_actions.SnsAction(self.alerts_topic))

        age_alarm = cloudwatch.Alarm(
            self,
            f"{identifier}QueueAgeAlarm",
            alarm_name=f"{self.env_name}-{identifier}-queue-age",
            alarm_description=f"{pipeline_name} ingestion queue oldest message age high",
            metric=age_metric,
            threshold=resolved_age_threshold,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        )
        age_alarm.add_alarm_action(cw_actions.SnsAction(self.alerts_topic))

        errors_alarm = cloudwatch.Alarm(
            self,
            f"{identifier}WorkerErrorsAlarm",
            alarm_name=f"{self.env_name}-{identifier}-worker-errors",
            alarm_description=f"{pipeline_name} ingestion worker errors detected",
            metric=errors_metric,
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        )
        errors_alarm.add_alarm_action(cw_actions.SnsAction(self.alerts_topic))

        throttles_alarm = cloudwatch.Alarm(
            self,
            f"{identifier}WorkerThrottlesAlarm",
            alarm_name=f"{self.env_name}-{identifier}-worker-throttles",
            alarm_description=f"{pipeline_name} ingestion worker throttles detected",
            metric=throttles_metric,
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        )
        throttles_alarm.add_alarm_action(cw_actions.SnsAction(self.alerts_topic))

        section_title = f"{pipeline_name} Ingestion"
        self.platform_dashboard.add_widgets(
            cloudwatch.TextWidget(markdown=f"### {section_title}", width=24),
            cloudwatch.GraphWidget(title="SQS Depth", left=[depth_metric], width=12, height=6),
            cloudwatch.GraphWidget(title="SQS Oldest Age", left=[age_metric], width=12, height=6),
            cloudwatch.GraphWidget(
                title="Worker Errors/Throttles", left=[errors_metric, throttles_metric], width=24, height=6
            ),
        )

    def _create_alerts_topic(self) -> sns.Topic:
        """Create single SNS topic for all alerts - simplified for small teams."""
        return sns.Topic(
            self,
            "AlertsTopic",
            topic_name=f"{self.env_name}-data-platform-alerts",
            display_name="Data Platform Alerts",
        )

    def _create_platform_dashboard(self) -> cloudwatch.Dashboard:
        """Create unified CloudWatch dashboard."""
        dashboard = cloudwatch.Dashboard(
            self,
            "PlatformDashboard",
            dashboard_name=f"{self.env_name}-data-platform-overview",
        )

        # Essential metrics widget - simplified view
        essential_metrics_widget = cloudwatch.GraphWidget(
            title="Data Pipeline Overview",
            width=24,
            height=8,
            left=[
                cloudwatch.Metric(
                    namespace="AWS/Lambda",
                    metric_name="Errors",
                    statistic="Sum",
                    label="Lambda Errors",
                ),
                cloudwatch.Metric(
                    namespace="AWS/States",
                    metric_name="ExecutionsFailed",
                    statistic="Sum",
                    label="Step Functions Failures",
                ),
            ],
            right=[
                cloudwatch.Metric(
                    namespace="AWS/Lambda",
                    metric_name="Invocations",
                    statistic="Sum",
                    label="Lambda Invocations",
                ),
                cloudwatch.Metric(
                    namespace="AWS/States",
                    metric_name="ExecutionsSucceeded",
                    statistic="Sum",
                    label="Step Functions Success",
                ),
            ],
        )

        dashboard.add_widgets(essential_metrics_widget)

        return dashboard

    def _create_essential_alarms(self) -> None:
        """Create only essential alarms for small teams."""
        # Step Functions failure alarm - most critical for data pipelines
        sf_failure_alarm = cloudwatch.Alarm(
            self,
            "PipelineFailures",
            alarm_name=f"{self.env_name}-data-pipeline-failures",
            alarm_description="Data pipeline Step Functions executions failing",
            metric=cloudwatch.Metric(
                namespace="AWS/States",
                metric_name="ExecutionsFailed",
                statistic="Sum",
            ),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=(cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD),
        )

        sf_failure_alarm.add_alarm_action(cw_actions.SnsAction(self.alerts_topic))

    def _create_glue_job_alarms(self) -> None:
        """Create CloudWatch alarms for monitored Glue jobs."""

        job_suffixes = list(self.config.get("monitored_glue_jobs", []))
        if not job_suffixes:
            return

        for suffix in job_suffixes:
            job_suffix = str(suffix).strip()
            if not job_suffix:
                continue

            job_name = f"{self.env_name}-{job_suffix}"
            alarm = cloudwatch.Alarm(
                self,
                f"GlueJobFailures-{job_suffix}",
                alarm_name=f"{job_name}-failures",
                alarm_description=f"Glue job {job_name} failed",
                metric=cloudwatch.Metric(
                    namespace="Glue",
                    metric_name="glue.jobrun.failed",
                    statistic="Sum",
                    dimensions_map={"JobName": job_name},
                    period=Duration.minutes(5),
                ),
                threshold=1,
                evaluation_periods=1,
                comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            )
            alarm.add_alarm_action(cw_actions.SnsAction(self.alerts_topic))

    def _create_state_machine_alarms(self) -> None:
        """Create targeted alarms for monitored Step Functions state machines."""

        state_machines = list(self.config.get("monitored_state_machines", []))
        if not state_machines:
            return

        account = Stack.of(self).account
        region = Stack.of(self).region

        for sm_suffix in state_machines:
            suffix = str(sm_suffix).strip()
            if not suffix:
                continue

            state_machine_name = f"{self.env_name}-{suffix}"
            state_machine_arn = f"arn:aws:states:{region}:{account}:stateMachine:{state_machine_name}"

            alarm = cloudwatch.Alarm(
                self,
                f"StateMachineFailures-{suffix}",
                alarm_name=f"{state_machine_name}-failures",
                alarm_description=f"State machine {state_machine_name} failures",
                metric=cloudwatch.Metric(
                    namespace="AWS/States",
                    metric_name="ExecutionsFailed",
                    statistic="Sum",
                    dimensions_map={"StateMachineArn": state_machine_arn},
                    period=Duration.minutes(5),
                ),
                threshold=1,
                evaluation_periods=1,
                comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            )
            alarm.add_alarm_action(cw_actions.SnsAction(self.alerts_topic))

    def _create_outputs(self) -> None:
        """Create CloudFormation outputs."""
        CfnOutput(
            self,
            "AlertsTopicArn",
            value=self.alerts_topic.topic_arn,
            description="Data platform alerts SNS topic ARN",
        )

        CfnOutput(
            self,
            "PlatformDashboardUrl",
            value=(
                f"https://console.aws.amazon.com/cloudwatch/home?region={self.region}"
                f"#dashboards:name={self.platform_dashboard.dashboard_name}"
            ),
            description="Platform dashboard URL",
        )
