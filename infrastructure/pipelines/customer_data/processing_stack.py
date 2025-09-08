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
        """Create Step Functions workflow for customer data processing."""
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

        # ETL job task
        etl_task = tasks.GlueStartJobRun(
            self,
            "ProcessCustomerData",
            glue_job_name=self.etl_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_json_path_at("$.glue_args"),
        )
        # Catch Glue task failures and route to normalized fail chain
        etl_task.add_catch(handler=fail_chain, result_path="$.error")

        # Data quality check task
        quality_check_task = tasks.LambdaInvoke(
            self,
            "QualityCheckCustomerData",
            lambda_function=self._create_quality_check_function(),
            output_path="$.Payload",
        )
        quality_check_task.add_catch(handler=fail_chain, result_path="$.error")

        # Schema check task (decide whether to run crawler)
        schema_check_task = tasks.LambdaInvoke(
            self,
            "SchemaCheckCustomerData",
            lambda_function=self._create_schema_check_function(),
            payload=sfn.TaskInput.from_object(
                {
                    "domain": self.config.get("ingestion_domain", "market"),
                    "table_name": self.config.get("ingestion_table_name", "prices"),
                    "catalog_update": self.config.get("catalog_update", "on_schema_change"),
                }
            ),
            output_path="$.Payload",
        )
        schema_check_task.add_catch(handler=fail_chain, result_path="$.error")

        # Conditional crawler start using AWS SDK integration
        crawler_name = f"{self.env_name}-curated-data-crawler"
        start_crawler_task = tasks.CallAwsService(
            self,
            "StartCrawlerIfNeeded",
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

        # Error handling via normalized fail_chain; branch-specific normalization below as needed

        # Define workflow
        # Prepare DQ failed branch for quality_check otherwise
        dq_failed_branch = sfn.Pass(
            self,
            "DQFailedBranch",
            parameters={"error": {"code": "DQ_FAILED", "message": "Quality check failed"}},
        ).next(fail_state)

        definition = preflight_task.next(
            sfn.Choice(self, "PreflightChoice")
            .when(
                sfn.Condition.boolean_equals("$.proceed", True),
                etl_task.next(quality_check_task).next(
                    sfn.Choice(self, "QualityChoice")
                    .when(
                        sfn.Condition.boolean_equals("$.quality_passed", True),
                        schema_check_task.next(
                            sfn.Choice(self, "CatalogUpdateChoice")
                            .when(
                                sfn.Condition.boolean_equals("$.should_crawl", True),
                                start_crawler_task.next(success_task),
                            )
                            .otherwise(success_task)
                        ),
                    )
                    .otherwise(dq_failed_branch)
                ),
            )
            .otherwise(
                sfn.Choice(self, "PreflightSkipOrError")
                .when(sfn.Condition.string_equals("$.error.code", "IDEMPOTENT_SKIP"), success_task)
                .otherwise(fail_chain)
            )
        )

        # Backfill (date_range) support: Build dates -> Map over ds -> reuse same processor
        build_dates_task = tasks.LambdaInvoke(
            self,
            "BuildDateArray",
            lambda_function=self._create_build_dates_function(),
            payload=sfn.TaskInput.from_object({"date_range.$": "$.date_range"}),
            output_path="$.Payload",
        )
        build_dates_task.add_catch(handler=fail_chain, result_path="$.error")

        map_state = sfn.Map(
            self,
            "BackfillMap",
            max_concurrency=int(self.config.get("backfill_max_concurrency", 2)),
            items_path=sfn.JsonPath.string_at("$.dates"),
            parameters={
                "domain.$": "$.domain",
                "table_name.$": "$.table_name",
                "file_type.$": "$.file_type",
                "ds.$": "$$.Map.Item.Value",
            },
        )

        preflight_map_task = tasks.LambdaInvoke(
            self,
            "PreflightForDs",
            lambda_function=preflight_task.lambda_function,  # reuse same function
            output_path="$.Payload",
        )
        preflight_map_task.add_catch(handler=fail_chain, result_path="$.error")

        etl_map_task = tasks.GlueStartJobRun(
            self,
            "ProcessCustomerDataDs",
            glue_job_name=self.etl_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_json_path_at("$.glue_args"),
        )
        etl_map_task.add_catch(handler=fail_chain, result_path="$.error")

        quality_map_task = tasks.LambdaInvoke(
            self,
            "QualityCheckCustomerDataDs",
            lambda_function=quality_check_task.lambda_function,  # reuse function
            output_path="$.Payload",
        )
        quality_map_task.add_catch(handler=fail_chain, result_path="$.error")

        schema_map_task = tasks.LambdaInvoke(
            self,
            "SchemaCheckCustomerDataDs",
            lambda_function=schema_check_task.lambda_function,  # reuse function
            payload=sfn.TaskInput.from_object(
                {
                    "domain.$": "$.domain",
                    "table_name.$": "$.table_name",
                    "catalog_update": self.config.get("catalog_update", "on_schema_change"),
                }
            ),
            output_path="$.Payload",
        )
        schema_map_task.add_catch(handler=fail_chain, result_path="$.error")

        start_crawler_map_task = tasks.CallAwsService(
            self,
            "StartCrawlerIfNeededDs",
            service="glue",
            action="startCrawler",
            parameters={"Name": crawler_name},
            iam_resources=[f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:crawler/{crawler_name}"],
            result_path=sfn.JsonPath.DISCARD,
        )
        start_crawler_map_task.add_catch(handler=fail_chain, result_path="$.error")

        dq_failed_map_branch = sfn.Pass(
            self,
            "DQFailedBranchDs",
            parameters={"error": {"code": "DQ_FAILED", "message": "Quality check failed"}},
        ).next(fail_state)

        map_item_chain = preflight_map_task.next(
            sfn.Choice(self, "PreflightChoiceDs")
            .when(
                sfn.Condition.boolean_equals("$.proceed", True),
                etl_map_task.next(quality_map_task).next(
                    sfn.Choice(self, "QualityChoiceDs")
                    .when(
                        sfn.Condition.boolean_equals("$.quality_passed", True),
                        schema_map_task.next(
                            sfn.Choice(self, "CatalogUpdateChoiceDs")
                            .when(
                                sfn.Condition.boolean_equals("$.should_crawl", True),
                                start_crawler_map_task,
                            )
                            .otherwise(sfn.Pass(self, "SkipCrawlerDs"))
                        ),
                    )
                    .otherwise(dq_failed_map_branch)
                ),
            )
            .otherwise(
                sfn.Choice(self, "PreflightSkipOrErrorDs")
                .when(
                    sfn.Condition.string_equals("$.error.code", "IDEMPOTENT_SKIP"), sfn.Pass(self, "SkipIdempotentDs")
                )
                .otherwise(fail_chain)
            )
        )
        map_state.item_processor(map_item_chain)

        backfill_chain = build_dates_task.next(map_state).next(success_task)

        # Choose backfill vs single-run
        top_definition = (
            sfn.Choice(self, "BackfillOrSingle")
            .when(sfn.Condition.is_present("$.date_range.start"), backfill_chain)
            .otherwise(definition)
        )

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

    def _create_validation_function(self) -> lambda_.IFunction:
        """Create data validation Lambda function wired to real handler."""
        function = PythonFunction(
            self,
            "CustomerDataValidationFunction",
            function_name=f"{self.env_name}-customer-data-validation",
            runtime=lambda_.Runtime.PYTHON_3_12,
            entry="src/lambda/functions/data_validator",
            index="handler.py",
            handler="main",
            memory_size=self._lambda_memory(),
            timeout=self._lambda_timeout(),
            log_retention=self._log_retention(),
            role=iam.Role.from_role_arn(self, "ValidationLambdaRole", self.lambda_execution_role_arn),
            layers=[self.common_layer],
            environment={
                "ENVIRONMENT": self.env_name,
                "RAW_BUCKET": self.shared_storage.raw_bucket.bucket_name,
            },
        )

        # S3 권한은 SecurityStack의 LambdaExecutionRole에 최소권한으로 부여됨
        # (교차 스택 grant로 인한 순환 참조를 피하기 위해 여기서는 grant를 사용하지 않음)
        return function

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

    def _create_quality_check_function(self) -> lambda_.Function:
        """Create data quality check Lambda function."""
        function = lambda_.Function(
            self,
            "CustomerDataQualityCheckFunction",
            function_name=f"{self.env_name}-customer-data-quality-check",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_inline(
                "def lambda_handler(event, context): return {'quality_passed': True}"
            ),  # Placeholder
            memory_size=self._lambda_memory(),
            timeout=self._lambda_timeout(),
            log_retention=self._log_retention(),
            role=iam.Role.from_role_arn(self, "QualityCheckLambdaRole", self.lambda_execution_role_arn),
            layers=[self.common_layer],
            environment={
                "ENVIRONMENT": self.env_name,
                "CURATED_BUCKET": self.shared_storage.curated_bucket.bucket_name,
            },
        )

        # S3 권한은 SecurityStack의 LambdaExecutionRole에 최소권한으로 부여됨
        # (교차 스택 grant로 인한 순환 참조를 피하기 위해 여기서는 grant를 사용하지 않음)
        return function

    def _create_schema_check_function(self) -> lambda_.IFunction:
        """Create schema change check Lambda function."""
        function = PythonFunction(
            self,
            "CustomerDataSchemaCheckFunction",
            function_name=f"{self.env_name}-customer-data-schema-check",
            runtime=lambda_.Runtime.PYTHON_3_12,
            entry="src/lambda/functions/schema_check",
            index="handler.py",
            handler="lambda_handler",
            memory_size=self._lambda_memory(),
            timeout=self._lambda_timeout(),
            log_retention=self._log_retention(),
            role=iam.Role.from_role_arn(self, "SchemaCheckLambdaRole", self.lambda_execution_role_arn),
            layers=[self.common_layer],
            environment={
                "ENVIRONMENT": self.env_name,
                "CURATED_BUCKET": self.shared_storage.curated_bucket.bucket_name,
                "ARTIFACTS_BUCKET": self.shared_storage.artifacts_bucket.bucket_name,
                "CATALOG_UPDATE_DEFAULT": str(self.config.get("catalog_update", "on_schema_change")),
            },
        )

        return function

    def _create_build_dates_function(self) -> lambda_.IFunction:
        """Create Lambda to build date array for backfill Map."""
        function = PythonFunction(
            self,
            "BuildDatesFunction",
            function_name=f"{self.env_name}-build-dates",
            runtime=lambda_.Runtime.PYTHON_3_12,
            entry="src/lambda/functions/build_dates",
            index="handler.py",
            handler="lambda_handler",
            memory_size=self._lambda_memory(),
            timeout=self._lambda_timeout(),
            log_retention=self._log_retention(),
            role=iam.Role.from_role_arn(self, "BuildDatesLambdaRole", self.lambda_execution_role_arn),
            layers=[self.common_layer],
            environment={
                "ENVIRONMENT": self.env_name,
                "MAX_BACKFILL_DAYS": str(int(self.config.get("max_backfill_days", 31))),
            },
        )
        return function

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
