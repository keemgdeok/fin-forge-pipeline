"""Unified observability stack for serverless data platform."""

from aws_cdk import (
    Stack,
    aws_cloudwatch as cloudwatch,
    aws_sns as sns,
    aws_cloudwatch_actions as cw_actions,
    aws_kms as kms,
    RemovalPolicy,
    # aws_logs as logs,  # OPTIONAL: Only if custom log groups needed
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

    def _create_alerts_topic(self) -> sns.Topic:
        """Create single SNS topic for all alerts - simplified for small teams."""
        # KMS key for topic encryption (basic hardening)
        key_removal = (
            RemovalPolicy.RETAIN
            if (self.env_name == "prod" or str(self.config.get("removal_policy", "retain")).lower() == "retain")
            else RemovalPolicy.DESTROY
        )
        topic_key = kms.Key(
            self,
            "AlertsTopicKey",
            enable_key_rotation=True,
            removal_policy=key_removal,
        )

        return sns.Topic(
            self,
            "AlertsTopic",
            topic_name=f"{self.env_name}-data-platform-alerts",
            display_name="Data Platform Alerts",
            master_key=topic_key,
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

    # OPTIONAL: AWS automatically creates log groups with default retention
    # def _create_log_groups(self) -> dict:
    #     """Create standardized log groups for platform components."""
    #     log_groups = {}
    #
    #     # Platform-wide log groups
    #     components = ["lambda", "glue", "stepfunctions", "pipeline-orchestration"]
    #
    #     for component in components:
    #         log_groups[component] = logs.LogGroup(
    #             self,
    #             f"{component.title()}LogGroup",
    #             log_group_name=
    #                 f"/aws/{component}/{self.env_name}-data-platform",
    #             retention=(
    #                 logs.RetentionDays.ONE_MONTH
    #                 if self.env_name != "prod"
    #                 else logs.RetentionDays.THREE_MONTHS
    #             ),
    #         )
    #
    #     return log_groups

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
