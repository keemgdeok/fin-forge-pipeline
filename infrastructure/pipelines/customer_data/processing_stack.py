"""Customer data processing pipeline stack."""

from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_logs as logs,
    aws_s3_assets as s3_assets,
    Duration,
    CfnOutput,
)
from constructs import Construct
from aws_cdk.aws_lambda_python_alpha import PythonFunction


class CustomerDataProcessingStack(Stack):
    """Customer data ETL processing pipeline."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: str,
        config: dict,
        shared_storage_stack,
        lambda_execution_role_arn: str,
        glue_execution_role_arn: str,
        step_functions_execution_role_arn: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.env_name = environment
        self.config = config
        self.shared_storage = shared_storage_stack
        self.lambda_execution_role_arn = lambda_execution_role_arn
        self.glue_execution_role_arn = glue_execution_role_arn
        self.step_functions_execution_role_arn = step_functions_execution_role_arn

        # Deterministic Glue job name used across resources (avoids Optional[str] typing)
        self.etl_job_name: str = f"{self.env_name}-customer-data-etl"

        # Common Layer for shared modules
        self.common_layer = self._create_common_layer()

        # Glue ETL job for customer data transformation
        self.etl_job = self._create_etl_job()

        # Feature toggle: processing orchestration (SFN + EB Rule)
        self.enable_processing: bool = bool(self.config.get("enable_processing_orchestration", False))

        if self.enable_processing:
            # Step Functions workflow for orchestration
            self.processing_workflow = self._create_processing_workflow()
            # EventBridge rules: S3 Object Created -> Start State Machine (multi prefix/suffix)
            self.s3_to_sfn_rules = self._create_s3_to_sfn_rules()

        self._create_outputs()

    def _create_etl_job(self) -> glue.CfnJob:
        """Create Glue ETL job for customer data processing."""
        # Package Glue script as a CDK asset and reference its S3 location
        glue_script_asset = s3_assets.Asset(
            self,
            "CustomerTransformScriptAsset",
            path="src/glue/jobs/customer_data_etl.py",
        )
        # Ensure Glue execution role can read the script asset
        glue_exec_role_ref = iam.Role.from_role_arn(self, "GlueExecRoleRefForScript", self.glue_execution_role_arn)
        glue_script_asset.grant_read(glue_exec_role_ref)

        domain: str = str(self.config.get("ingestion_domain", "market"))
        table_name: str = str(self.config.get("ingestion_table_name", "prices"))

        raw_prefix = f"{domain}/{table_name}/"
        curated_prefix = f"{domain}/{table_name}/"

        # Schema fingerprint artifacts path aligned to spec
        schema_fp_uri = (
            f"s3://{self.shared_storage.artifacts_bucket.bucket_name}/{domain}/{table_name}/_schema/latest.json"
        )

        return glue.CfnJob(
            self,
            "CustomerETLJob",
            name=self.etl_job_name,
            role=self.glue_execution_role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=glue_script_asset.s3_object_url,
                python_version="3",
            ),
            default_arguments={
                "--job-language": "python",
                "--job-bookmark-option": "job-bookmark-enable",
                "--enable-s3-parquet-optimized-committer": "1",
                "--codec": "zstd",
                "--target_file_mb": "256",
                "--TempDir": (f"s3://{self.shared_storage.artifacts_bucket.bucket_name}" "/temp/"),
                "--raw_bucket": self.shared_storage.raw_bucket.bucket_name,
                "--raw_prefix": raw_prefix,
                "--curated_bucket": self.shared_storage.curated_bucket.bucket_name,
                "--curated_prefix": curated_prefix,
                "--environment": self.env_name,
                "--schema_fingerprint_s3_uri": schema_fp_uri,
            },
            glue_version="4.0",
            max_retries=1,  # Spec: 1 retry
            timeout=30,  # Spec: 30 minutes
            worker_type="G.1X",
            number_of_workers=int(self.config.get("glue_max_capacity", 2)),
        )

    def _create_processing_workflow(self) -> sfn.StateMachine:
        """Create simplified Step Functions workflow for 1GB daily batch processing.

        Simplified architecture:
        1. Preflight Lambda: ds extraction, idempotency check, Glue args
        2. Glue ETL Job: data transformation + ALL DQ validation integrated
        3. Glue Crawler: automatic schema detection (uses native RecrawlPolicy)

        Removed complexity:
        - Data Validator Lambda (redundant with Glue DQ)
        - Quality Check Lambda (placeholder, redundant)
        - Schema Check Lambda (replaced by Glue Crawler native features)
        - Build Dates Lambda (1 daily batch doesn't need backfill)
        """
        # Preflight task: derive ds, idempotency skip, build Glue args
        preflight_task = tasks.LambdaInvoke(
            self,
            "PreflightCustomerData",
            lambda_function=self._create_preflight_function(),
            output_path="$.Payload",
        )

        # Common failure normalization: shape error payload and then fail once
        normalize_fail = sfn.Pass(
            self,
            "NormalizeAndFail",
            parameters={
                "ok": False,
                "error.$": "$.error",
            },
        )
        fail_state = sfn.Fail(self, "ExecutionFailed", comment="Pipeline execution failed")
        fail_chain = normalize_fail.next(fail_state)

        # ETL job task (now includes ALL data quality validation)
        etl_task = tasks.GlueStartJobRun(
            self,
            "ProcessCustomerData",
            glue_job_name=self.etl_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_json_path_at("$.glue_args"),
        )
        # Catch Glue task failures (includes DQ failures) and route to normalized fail chain
        etl_task.add_catch(handler=fail_chain, result_path="$.error")

        # Crawler task: automatic schema detection using native AWS features
        crawler_name = f"{self.env_name}-curated-data-crawler"
        start_crawler_task = tasks.CallAwsService(
            self,
            "StartCrawler",
            service="glue",
            action="startCrawler",
            parameters={"Name": crawler_name},
            iam_resources=[f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:crawler/{crawler_name}"],
            result_path=sfn.JsonPath.DISCARD,
        )
        start_crawler_task.add_catch(handler=fail_chain, result_path="$.error")

        # Success notification
        success_task = sfn.Succeed(
            self,
            "CustomerDataProcessingSuccess",
            comment="Customer data processing completed successfully",
        )

        # Simplified linear workflow: Preflight -> ETL+DQ -> Crawler -> Success
        definition = preflight_task.next(
            sfn.Choice(self, "PreflightChoice")
            .when(
                sfn.Condition.boolean_equals("$.proceed", True),
                etl_task.next(start_crawler_task).next(success_task),
            )
            .otherwise(
                sfn.Choice(self, "PreflightSkipOrError")
                .when(sfn.Condition.string_equals("$.error.code", "IDEMPOTENT_SKIP"), success_task)
                .otherwise(fail_chain)
            )
        )

        # Removed backfill support: 1GB daily batch doesn't need complex date range processing
        # If backfill is needed later, can be handled by manual re-runs with different ds values
        top_definition = definition

        # Create log group for the state machine with retention from config
        sm_log_group = logs.LogGroup(
            self,
            "ProcessingStateMachineLogs",
            retention=self._log_retention(),
        )

        return sfn.StateMachine(
            self,
            "CustomerDataProcessingWorkflow",
            state_machine_name=f"{self.env_name}-customer-data-processing",
            definition_body=sfn.DefinitionBody.from_chainable(top_definition),
            role=iam.Role.from_role_arn(
                self,
                "StepFunctionsExecutionRoleRef",
                self.step_functions_execution_role_arn,
            ),
            logs=sfn.LogOptions(destination=sm_log_group, level=sfn.LogLevel.ALL, include_execution_data=True),
            tracing_enabled=bool(self.config.get("enable_xray_tracing", False)),
            timeout=Duration.hours(2),
        )

    def _create_s3_to_sfn_rules(self) -> list[events.Rule]:
        """Create one or more EventBridge rules to invoke the state machine.

        Supports multi-domain/table by accepting a list of trigger configs in env config
        under key 'processing_triggers'. Falls back to a single trigger derived from
        ingestion defaults if not provided.
        """
        triggers: list[dict] = list(self.config.get("processing_triggers", []))

        if not triggers:
            triggers = [
                {
                    "domain": self.config.get("ingestion_domain", "market"),
                    "table_name": self.config.get("ingestion_table_name", "prices"),
                    "file_type": self.config.get("ingestion_file_format", "json"),
                    "suffixes": self.config.get("processing_suffixes", [".json", ".csv"]),
                }
            ]

        created: list[events.Rule] = []
        for t in triggers:
            domain: str = str(t.get("domain", "")).strip()
            table_name: str = str(t.get("table_name", "")).strip()
            file_type: str = str(t.get("file_type", "json")).strip() or "json"
            suffixes: list[str] = list(t.get("suffixes", [".json", ".csv"]))

            if not domain or not table_name:
                # Skip malformed trigger
                continue

            rule = self._add_s3_to_sfn_rule(domain, table_name, file_type, suffixes)
            created.append(rule)

        return created

    def _add_s3_to_sfn_rule(self, domain: str, table_name: str, file_type: str, suffixes: list[str]) -> events.Rule:
        """Create a single EventBridge rule for a specific domain/table and suffix set."""
        prefix = f"{domain}/{table_name}/"

        # Build key filters: require prefix AND any-of suffixes, use one entry per suffix
        key_filters = [{"prefix": prefix, "suffix": s} for s in suffixes]

        rule_id = f"RawObjectCreated-{domain}-{table_name}".replace("/", "-")
        rule = events.Rule(
            self,
            rule_id,
            rule_name=f"{self.env_name}-raw-object-created-{domain}-{table_name}",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [self.shared_storage.raw_bucket.bucket_name]},
                    "object": {"key": key_filters},
                },
            ),
        )

        rule.add_target(
            targets.SfnStateMachine(
                self.processing_workflow,
                input=events.RuleTargetInput.from_object(
                    {
                        "source_bucket": events.EventField.from_path("$.detail.bucket.name"),
                        "source_key": events.EventField.from_path("$.detail.object.key"),
                        "table_name": table_name,
                        "domain": domain,
                        "file_type": file_type,
                    }
                ),
            )
        )

        return rule

    # Removed: _create_validation_function - redundant with Glue ETL DQ validation

    def _create_preflight_function(self) -> lambda_.IFunction:
        """Create preflight Lambda function for ds/idempotency/args."""
        function = PythonFunction(
            self,
            "CustomerDataPreflightFunction",
            function_name=f"{self.env_name}-customer-data-preflight",
            runtime=lambda_.Runtime.PYTHON_3_12,
            entry="src/lambda/functions/preflight",
            index="handler.py",
            handler="lambda_handler",
            memory_size=self._lambda_memory(),
            timeout=self._lambda_timeout(),
            log_retention=self._log_retention(),
            role=iam.Role.from_role_arn(self, "PreflightLambdaRole", self.lambda_execution_role_arn),
            layers=[self.common_layer],
            environment={
                "ENVIRONMENT": self.env_name,
                "RAW_BUCKET": self.shared_storage.raw_bucket.bucket_name,
                "CURATED_BUCKET": self.shared_storage.curated_bucket.bucket_name,
                "ARTIFACTS_BUCKET": self.shared_storage.artifacts_bucket.bucket_name,
            },
        )
        return function

    # Removed: _create_quality_check_function - placeholder function, DQ now in Glue ETL

    # Removed: _create_schema_check_function - replaced by Glue Crawler native RecrawlPolicy

    # Removed: _create_build_dates_function - 1GB daily batch doesn't need backfill complexity

    def _create_outputs(self) -> None:
        """Create CloudFormation outputs."""
        CfnOutput(
            self,
            "ETLJobName",
            value=self.etl_job_name,
            description="Customer data ETL job name",
        )

        # Output only when processing is enabled
        if getattr(self, "enable_processing", False) and hasattr(self, "processing_workflow"):
            CfnOutput(
                self,
                "ProcessingWorkflowArn",
                value=self.processing_workflow.state_machine_arn,
                description="Customer data processing workflow ARN",
            )

    def _create_common_layer(self) -> lambda_.LayerVersion:
        """Create Common Layer for shared models and utils."""
        return lambda_.LayerVersion(
            self,
            "CommonLayer",
            layer_version_name=f"{self.env_name}-common-layer",
            description="Shared common models and utilities",
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            code=lambda_.Code.from_asset("src/lambda/layers/common"),
        )

    # ===== Helpers =====
    def _log_retention(self) -> logs.RetentionDays:
        """Map integer days from config to CloudWatch Logs retention enum."""
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

    def _lambda_memory(self) -> int:
        """Resolve Lambda memory size from config (MB)."""
        return int(self.config.get("lambda_memory", 512))

    def _lambda_timeout(self) -> Duration:
        """Resolve Lambda timeout from config (seconds)."""
        return Duration.seconds(int(self.config.get("lambda_timeout", 300)))
