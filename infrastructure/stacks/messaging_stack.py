"""Messaging stack for EventBridge, SQS, and SNS."""
from aws_cdk import (
    Stack,
    aws_events as events,
    aws_events_targets as targets,
    aws_sns as sns,
    aws_sqs as sqs,
    Duration,
)
from constructs import Construct


class MessagingStack(Stack):
    """Stack containing messaging services for the data pipeline."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: str,
        config: dict,
        orchestration_stack,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.environment = environment
        self.config = config
        self.orchestration_stack = orchestration_stack

        # Create messaging components
        self._create_sns_topics()
        self._create_sqs_queues()
        self._create_eventbridge_rules()

    def _create_sns_topics(self) -> None:
        """Create SNS topics for notifications."""
        self.pipeline_notifications_topic = sns.Topic(
            self, "PipelineNotifications",
            topic_name=f"data-pipeline-notifications-{self.environment}",
            display_name=f"Data Pipeline Notifications ({self.environment})",
        )

        self.error_notifications_topic = sns.Topic(
            self, "ErrorNotifications",
            topic_name=f"data-pipeline-errors-{self.environment}",
            display_name=f"Data Pipeline Error Notifications ({self.environment})",
        )

    def _create_sqs_queues(self) -> None:
        """Create SQS queues for message processing."""
        # Dead letter queue for failed messages
        self.dlq = sqs.Queue(
            self, "DeadLetterQueue",
            queue_name=f"data-pipeline-dlq-{self.environment}",
            retention_period=Duration.days(14),
        )

        # Main processing queue
        self.processing_queue = sqs.Queue(
            self, "ProcessingQueue",
            queue_name=f"data-pipeline-processing-{self.environment}",
            visibility_timeout=Duration.minutes(15),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=self.dlq,
            ),
        )

    def _create_eventbridge_rules(self) -> None:
        """Create EventBridge rules to trigger the pipeline."""
        # Rule for scheduled pipeline execution
        self.scheduled_rule = events.Rule(
            self, "ScheduledRule",
            rule_name=f"data-pipeline-scheduled-{self.environment}",
            description="Scheduled execution of data pipeline",
            schedule=events.Schedule.rate(Duration.hours(24)),  # Daily execution
            enabled=self.environment == "prod",  # Only enable in production
        )

        if hasattr(self.orchestration_stack, 'step_function'):
            self.scheduled_rule.add_target(
                targets.SfnStateMachine(
                    self.orchestration_stack.step_function,
                    input=events.RuleTargetInput.from_object({
                        "trigger_type": "scheduled",
                        "environment": self.environment,
                    }),
                )
            )