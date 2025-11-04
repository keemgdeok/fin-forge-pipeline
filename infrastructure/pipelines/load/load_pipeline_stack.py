"""Load pipeline stack: S3 curated events → SQS queues for on-prem loader."""

from __future__ import annotations

import json
from typing import Dict, List

from aws_cdk import (
    Duration,
    Stack,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_lambda_python_alpha as lambda_python,
    aws_logs as logs,
    aws_sqs as sqs,
    aws_cloudwatch as cw,
    CfnOutput,
)
from constructs import Construct


class LoadPipelineStack(Stack):
    """Provision SQS queues and EventBridge rules for the load (pull) pipeline."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        environment: str,
        config: Dict,
        shared_storage_stack,
        lambda_execution_role_arn: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.env_name = environment
        self.config = config
        self.shared_storage = shared_storage_stack
        self.lambda_execution_role_arn = lambda_execution_role_arn

        self.allowed_layers: List[str] = list(self.config.get("allowed_load_layers", []))
        domain_configs: List[Dict] = list(self.config.get("load_domain_configs", []))
        if not domain_configs:
            raise ValueError("load_domain_configs must be defined in environment configuration")

        self.load_layer = self._create_load_layer()
        self.queues: Dict[str, sqs.Queue] = {}
        self.dlqs: Dict[str, sqs.Queue] = {}

        for domain_cfg in domain_configs:
            self._create_domain_resources(domain_cfg)

        self.publisher_function = self._create_event_publisher_lambda(domain_configs)
        self._create_eventbridge_rules(domain_configs)
        self._create_alarms(domain_configs)
        self._create_outputs(domain_configs)

    def _create_domain_resources(self, domain_cfg: Dict) -> None:
        domain = str(domain_cfg.get("domain", "")).strip()
        if not domain:
            raise ValueError("Each load_domain_config must include 'domain'")

        dlq = sqs.Queue(
            self,
            f"{domain.capitalize()}LoadDlq",
            queue_name=f"{self.env_name}-{domain}-load-dlq",
            retention_period=Duration.days(14),
            enforce_ssl=True,
        )

        visibility_seconds = int(domain_cfg.get("visibility_timeout_seconds", 1800))
        main_queue = sqs.Queue(
            self,
            f"{domain.capitalize()}LoadQueue",
            queue_name=f"{self.env_name}-{domain}-load-queue",
            visibility_timeout=Duration.seconds(visibility_seconds),
            retention_period=Duration.days(int(domain_cfg.get("message_retention_days", 14))),
            dead_letter_queue=sqs.DeadLetterQueue(
                queue=dlq,
                max_receive_count=int(domain_cfg.get("max_receive_count", 3)),
            ),
            enforce_ssl=True,
        )

        self.queues[domain] = main_queue
        self.dlqs[domain] = dlq

    def _create_event_publisher_lambda(self, domain_configs: List[Dict]) -> lambda_python.PythonFunction:
        queue_map = {domain_cfg["domain"]: self.queues[domain_cfg["domain"]].queue_url for domain_cfg in domain_configs}
        priority_map = {domain_cfg["domain"]: str(domain_cfg.get("priority", "3")) for domain_cfg in domain_configs}

        function = lambda_python.PythonFunction(
            self,
            "LoadEventPublisher",
            function_name=f"{self.env_name}-load-event-publisher",
            runtime=lambda_.Runtime.PYTHON_3_12,
            entry="src/lambda/functions/load_event_publisher",
            index="handler.py",
            handler="main",
            memory_size=int(self.config.get("load_publisher_memory", 128)),
            timeout=Duration.seconds(int(self.config.get("load_publisher_timeout", 30))),
            log_retention=self._log_retention(),
            role=iam.Role.from_role_arn(
                self,
                "LoadPublisherRole",
                self.lambda_execution_role_arn,
                mutable=False,
            ),
            environment={
                "ENVIRONMENT": self.env_name,
                "LOAD_QUEUE_MAP": json.dumps(queue_map),
                "PRIORITY_MAP": json.dumps(priority_map),
                "MIN_FILE_SIZE_BYTES": str(int(self.config.get("load_min_file_size_bytes", 1024))),
                "ALLOWED_LAYERS": json.dumps(self.allowed_layers),
            },
            layers=[self.load_layer],
        )

        return function

    def _create_load_layer(self) -> lambda_.ILayerVersion:
        return lambda_.LayerVersion(
            self,
            "LoadContractsLayer",
            code=lambda_.Code.from_asset("src/lambda/layers/load/contracts"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            description="Shared load pipeline contracts",
        )

    def _create_eventbridge_rules(self, domain_configs: List[Dict]) -> None:
        curated_bucket_name = self.shared_storage.curated_bucket.bucket_name

        for domain_cfg in domain_configs:
            domain = domain_cfg["domain"]
            prefix = str(domain_cfg.get("s3_prefix", f"{domain}/"))
            rule = events.Rule(
                self,
                f"{domain.capitalize()}LoadRule",
                rule_name=f"{self.env_name}-{domain}-load-rule",
                event_pattern=events.EventPattern(
                    source=["aws.s3"],
                    detail_type=["Object Created"],
                    detail={
                        "bucket": {"name": [curated_bucket_name]},
                        "object": {
                            "key": [{"prefix": prefix}],
                            "size": [{"numeric": [">=", int(self.config.get("load_min_file_size_bytes", 1024))]}],
                        },
                    },
                ),
            )

            rule.add_target(
                targets.LambdaFunction(
                    self.publisher_function,
                    event=events.RuleTargetInput.from_event_path("$"),
                    dead_letter_queue=self.dlqs[domain],
                    retry_attempts=int(self.config.get("load_eventbridge_retry_attempts", 0)),
                )
            )

    def _create_alarms(self, domain_configs: List[Dict]) -> None:
        for domain_cfg in domain_configs:
            domain = domain_cfg["domain"]
            queue = self.queues[domain]
            dlq = self.dlqs[domain]

            cw.Alarm(
                self,
                f"{domain.capitalize()}LoadQueueDepthAlarm",
                metric=queue.metric_approximate_number_of_messages_visible(),
                threshold=float(domain_cfg.get("queue_depth_alarm_threshold", 100)),
                evaluation_periods=2,
                datapoints_to_alarm=2,
                comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
                alarm_name=f"{self.env_name}-{domain}-load-queue-depth-high",
                alarm_description="SQS load queue depth is above threshold",
            )

            cw.Alarm(
                self,
                f"{domain.capitalize()}LoadDlqAlarm",
                metric=dlq.metric_approximate_number_of_messages_visible(),
                threshold=1,
                evaluation_periods=1,
                datapoints_to_alarm=1,
                comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
                alarm_name=f"{self.env_name}-{domain}-load-dlq-messages",
                alarm_description="Load DLQ has messages pending",
            )

    def _create_outputs(self, domain_configs: List[Dict]) -> None:
        for domain_cfg in domain_configs:
            domain = domain_cfg["domain"]
            CfnOutput(
                self,
                f"{domain.capitalize()}LoadQueueUrl",
                value=self.queues[domain].queue_url,
                description=f"SQS queue URL for {domain} load pipeline",
            )
            CfnOutput(
                self,
                f"{domain.capitalize()}LoadDlqUrl",
                value=self.dlqs[domain].queue_url,
                description=f"DLQ URL for {domain} load pipeline",
            )

    def _log_retention(self) -> logs.RetentionDays:
        retention_map = {
            1: logs.RetentionDays.ONE_DAY,
            3: logs.RetentionDays.THREE_DAYS,
            5: logs.RetentionDays.FIVE_DAYS,
            7: logs.RetentionDays.ONE_WEEK,
            14: logs.RetentionDays.TWO_WEEKS,
            30: logs.RetentionDays.ONE_MONTH,
            90: logs.RetentionDays.THREE_MONTHS,
        }
        return retention_map.get(self.config.get("log_retention_days", 14), logs.RetentionDays.TWO_WEEKS)
