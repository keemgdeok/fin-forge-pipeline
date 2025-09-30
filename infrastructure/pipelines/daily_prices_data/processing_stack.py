"""Daily prices processing pipeline stack."""

from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_logs as logs,
    aws_s3_assets as s3_assets,
    Duration,
    CfnOutput,
)
from constructs import Construct
from aws_cdk.aws_lambda_python_alpha import BundlingOptions, PythonFunction, PythonLayerVersion


class DailyPricesDataProcessingStack(Stack):
    """Daily prices ETL processing pipeline."""

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
        self.compaction_output_subdir: str = str(self.config.get("compaction_output_subdir", "compacted"))
        self.compaction_codec: str = str(self.config.get("compaction_codec", "zstd"))
        self.glue_max_concurrent_runs: int = int(self.config.get("glue_max_concurrent_runs", 1))
        self.glue_retry_interval_seconds: int = int(self.config.get("glue_retry_interval_seconds", 30))
        self.glue_retry_backoff_rate: float = float(self.config.get("glue_retry_backoff_rate", 2.0))
        self.glue_retry_max_attempts: int = int(self.config.get("glue_retry_max_attempts", 5))
        self.curated_layer: str = str(self.config.get("curated_layer_name", "adjusted"))
        self.indicators_layer: str = str(self.config.get("indicators_layer", "technical_indicator"))
        self.map_max_concurrency: int = int(self.config.get("sfn_max_concurrency", 1))

        # Deterministic Glue job name used across resources (avoids Optional[str] typing)
        self.etl_job_name: str = f"{self.env_name}-daily-prices-data-etl"

        # Common Layer for shared modules
        self.common_layer = self._create_common_layer()

        # Glue compaction job: RAW JSON -> Curated Parquet (pre-transform)
        self.compaction_job = self._create_compaction_job()

        # Glue ETL job for daily prices data transformation
        self.etl_job = self._create_etl_job()

        # Glue ETL job for indicators computation (Curated prices -> Curated indicators)
        self.indicators_job = self._create_indicators_job()

        # Step Functions workflow for orchestrating manifest-driven processing
        self.processing_workflow = self._create_processing_workflow()

        self._create_outputs()

    def _create_compaction_job(self) -> glue.CfnJob:
        """Create Glue job to compact RAW JSON into Curated Parquet."""

        compaction_script_asset = s3_assets.Asset(
            self,
            "DailyPricesCompactionScriptAsset",
            path="src/glue/jobs/raw_to_parquet_compaction.py",
        )
        glue_exec_role_ref = iam.Role.from_role_arn(
            self,
            "GlueExecRoleRefForCompaction",
            self.glue_execution_role_arn,
        )
        compaction_script_asset.grant_read(glue_exec_role_ref)

        shared_py_asset = s3_assets.Asset(
            self,
            "CompactionSharedPythonAsset",
            path="src/lambda/layers/common/python",
        )
        shared_py_asset.grant_read(glue_exec_role_ref)

        domain: str = str(self.config.get("ingestion_domain", "market"))
        table_name: str = str(self.config.get("ingestion_table_name", "prices"))

        raw_prefix = f"{domain}/{table_name}/"

        self.compaction_job_name = f"{self.env_name}-daily-prices-compaction"

        return glue.CfnJob(
            self,
            "DailyPricesCompactionJob",
            name=self.compaction_job_name,
            role=self.glue_execution_role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=compaction_script_asset.s3_object_url,
                python_version="3",
            ),
            default_arguments={
                "--job-language": "python",
                "--job-bookmark-option": "job-bookmark-disable",
                "--TempDir": f"s3://{self.shared_storage.artifacts_bucket.bucket_name}/temp/",
                "--raw_bucket": self.shared_storage.raw_bucket.bucket_name,
                "--raw_prefix": raw_prefix,
                "--compacted_bucket": self.shared_storage.curated_bucket.bucket_name,
                "--codec": self.compaction_codec,
                "--target_file_mb": str(int(self.config.get("compaction_target_file_mb", 256))),
                "--extra-py-files": shared_py_asset.s3_object_url,
            },
            glue_version="5.0",
            max_retries=1,
            timeout=int(self.config.get("compaction_timeout_minutes", 15)),
            worker_type=str(self.config.get("compaction_worker_type", "G.1X")),
            number_of_workers=int(self.config.get("compaction_number_workers", 2)),
            execution_property=glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs=self.glue_max_concurrent_runs),
        )

    def _create_etl_job(self) -> glue.CfnJob:
        """Create Glue ETL job for daily prices data processing."""
        # Package Glue script as a CDK asset and reference its S3 location
        glue_script_asset = s3_assets.Asset(
            self,
            "DailyPricesTransformScriptAsset",
            path="src/glue/jobs/daily_prices_data_etl.py",
        )
        # Ensure Glue execution role can read the script asset
        glue_exec_role_ref = iam.Role.from_role_arn(self, "GlueExecRoleRefForScript", self.glue_execution_role_arn)
        glue_script_asset.grant_read(glue_exec_role_ref)

        # Provide shared Python packages ("shared" module) to Glue via --extra-py-files
        # Point at the Layer's python root so the zip includes `shared/` at top-level
        shared_py_asset = s3_assets.Asset(
            self,
            "SharedPythonPackageAsset",
            path="src/lambda/layers/common/python",
        )
        shared_py_asset.grant_read(glue_exec_role_ref)

        domain: str = str(self.config.get("ingestion_domain", "market"))
        table_name: str = str(self.config.get("ingestion_table_name", "prices"))

        raw_prefix = f"{domain}/{table_name}/"

        # Schema fingerprint artifacts path aligned to spec
        schema_fp_uri = (
            f"s3://{self.shared_storage.artifacts_bucket.bucket_name}/{domain}/{table_name}/_schema/latest.json"
        )

        return glue.CfnJob(
            self,
            "DailyPricesETLJob",
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
                "--enable-s3-parquet-optimized-committer": "true",
                "--codec": "zstd",
                "--target_file_mb": "256",
                "--TempDir": (f"s3://{self.shared_storage.artifacts_bucket.bucket_name}/temp/"),
                # Make the shared Python package available at runtime (provides shared.dq.engine)
                "--extra-py-files": shared_py_asset.s3_object_url,
                "--raw_bucket": self.shared_storage.raw_bucket.bucket_name,
                "--raw_prefix": raw_prefix,
                "--compacted_bucket": self.shared_storage.curated_bucket.bucket_name,
                "--curated_bucket": self.shared_storage.curated_bucket.bucket_name,
                "--domain": domain,
                "--table_name": table_name,
                "--curated_layer": self.curated_layer,
                "--compacted_layer": self.compaction_output_subdir,
                "--interval": str(self.config.get("ingestion_interval", "1d")),
                "--data_source": str(self.config.get("ingestion_data_source", "yahoo_finance")),
                "--environment": self.env_name,
                "--schema_fingerprint_s3_uri": schema_fp_uri,
            },
            # Align with docs/spec: Glue 5.0
            glue_version="5.0",
            max_retries=1,  # Spec: 1 retry
            timeout=30,  # Spec: 30 minutes
            worker_type="G.1X",
            number_of_workers=int(self.config.get("glue_max_capacity", 2)),
            execution_property=glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs=self.glue_max_concurrent_runs),
        )

    def _create_indicators_job(self) -> glue.CfnJob:
        """Create Glue ETL job for market indicators computation from curated prices."""
        # Package Glue script as a CDK asset and reference its S3 location
        indicators_script_asset = s3_assets.Asset(
            self,
            "IndicatorsTransformScriptAsset",
            path="src/glue/jobs/market_indicators_etl.py",
        )
        glue_exec_role_ref = iam.Role.from_role_arn(self, "GlueExecRoleRefForIndicators", self.glue_execution_role_arn)
        indicators_script_asset.grant_read(glue_exec_role_ref)

        # Provide shared Python package and indicators lib to Glue via --extra-py-files
        shared_py_asset = s3_assets.Asset(
            self,
            "SharedPythonPackageAssetForIndicators",
            path="src/lambda/layers/common/python",
        )
        shared_py_asset.grant_read(glue_exec_role_ref)

        indicators_lib_asset = s3_assets.Asset(
            self,
            "IndicatorsLibAsset",
            path="src",
        )
        indicators_lib_asset.grant_read(glue_exec_role_ref)

        domain: str = str(self.config.get("ingestion_domain", "market"))
        prices_table: str = str(self.config.get("ingestion_table_name", "prices"))
        indicators_table: str = str(self.config.get("indicators_table_name", "indicators"))

        schema_fp_uri = f"s3://{self.shared_storage.artifacts_bucket.bucket_name}/{domain}/{prices_table}/{indicators_table}/_schema/latest.json"

        # Deterministic name for the indicators job
        self.indicators_job_name: str = f"{self.env_name}-market-indicators-etl"

        return glue.CfnJob(
            self,
            "IndicatorsETLJob",
            name=self.indicators_job_name,
            role=self.glue_execution_role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=indicators_script_asset.s3_object_url,
                python_version="3",
            ),
            default_arguments={
                "--job-language": "python",
                "--job-bookmark-option": "job-bookmark-enable",
                "--enable-s3-parquet-optimized-committer": "true",
                "--codec": "zstd",
                "--target_file_mb": "256",
                "--TempDir": (f"s3://{self.shared_storage.artifacts_bucket.bucket_name}/temp/"),
                "--extra-py-files": f"{shared_py_asset.s3_object_url},{indicators_lib_asset.s3_object_url}",
                # Inputs/outputs
                "--environment": self.env_name,
                "--domain": domain,
                "--table_name": prices_table,
                "--interval": str(self.config.get("ingestion_interval", "1d")),
                "--data_source": str(self.config.get("ingestion_data_source", "yahoo_finance")),
                "--prices_curated_bucket": self.shared_storage.curated_bucket.bucket_name,
                "--prices_layer": self.curated_layer,
                "--output_bucket": self.shared_storage.curated_bucket.bucket_name,
                "--output_layer": self.indicators_layer,
                "--schema_fingerprint_s3_uri": schema_fp_uri,
                # Window size
                "--lookback_days": str(int(self.config.get("indicators_lookback_days", 252))),
                # ds, codec, target_file_mb are provided per-run via arguments
            },
            glue_version="5.0",
            max_retries=1,
            timeout=30,
            worker_type="G.1X",
            number_of_workers=int(self.config.get("glue_max_capacity", 2)),
            execution_property=glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs=self.glue_max_concurrent_runs),
        )

    def _create_processing_workflow(self) -> sfn.StateMachine:
        """Create manifest-driven Step Functions workflow for sequential processing."""

        preflight_task = tasks.LambdaInvoke(
            self,
            "PreflightDailyPrices",
            lambda_function=self._create_preflight_function(),
            payload=sfn.TaskInput.from_object(
                {
                    "ds.$": "$.ds",
                    "domain.$": "$$.Execution.Input.domain",
                    "table_name.$": "$$.Execution.Input.table_name",
                    "file_type.$": "$$.Execution.Input.file_type",
                    "interval.$": "$$.Execution.Input.interval",
                    "data_source.$": "$$.Execution.Input.data_source",
                    "catalog_update.$": "$$.Execution.Input.catalog_update",
                    "source_bucket.$": "$$.Execution.Input.raw_bucket",
                    "source_key.$": "$.manifest_key",
                }
            ),
            payload_response_only=True,
        )

        compaction_guard_fn = self._create_compaction_guard_function()

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

        domain: str = str(self.config.get("ingestion_domain", "market"))
        prices_table: str = str(self.config.get("ingestion_table_name", "prices"))
        indicators_table: str = str(self.config.get("indicators_table_name", "indicators"))

        build_compaction_args = sfn.Pass(
            self,
            "BuildCompactionArgs",
            parameters={
                "--raw_bucket": sfn.JsonPath.string_at("$.glue_args['--raw_bucket']"),
                "--raw_prefix": sfn.JsonPath.string_at("$.glue_args['--raw_prefix']"),
                "--compacted_bucket": sfn.JsonPath.string_at("$.glue_args['--compacted_bucket']"),
                "--file_type": sfn.JsonPath.string_at("$.glue_args['--file_type']"),
                "--interval": sfn.JsonPath.string_at("$.glue_args['--interval']"),
                "--data_source": sfn.JsonPath.string_at("$.glue_args['--data_source']"),
                "--ds": sfn.JsonPath.string_at("$.ds"),
                "--domain": domain,
                "--table_name": prices_table,
                "--layer": self.compaction_output_subdir,
                "--codec": self.compaction_codec,
                "--target_file_mb": str(int(self.config.get("compaction_target_file_mb", 256))),
            },
            result_path="$.compaction_args",
        )

        compaction_task = tasks.GlueStartJobRun(
            self,
            "CompactRawDailyPrices",
            glue_job_name=self.compaction_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_json_path_at("$.compaction_args"),
            result_path=sfn.JsonPath.DISCARD,
        )
        compaction_task.add_catch(handler=fail_chain, result_path="$.error")

        etl_task = tasks.GlueStartJobRun(
            self,
            "ProcessDailyPrices",
            glue_job_name=self.etl_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_json_path_at("$.glue_args"),
            result_path="$.prices_etl",
        )
        etl_task.add_retry(
            errors=["Glue.ConcurrentRunsExceededException"],
            interval=Duration.seconds(self.glue_retry_interval_seconds),
            max_attempts=self.glue_retry_max_attempts,
            backoff_rate=self.glue_retry_backoff_rate,
        )
        etl_task.add_catch(handler=fail_chain, result_path="$.error")

        indicators_fp = (
            f"s3://{self.shared_storage.artifacts_bucket.bucket_name}/"
            f"{domain}/{prices_table}/{indicators_table}/_schema/latest.json"
        )

        build_indicators_args = sfn.Pass(
            self,
            "BuildIndicatorsArgs",
            parameters={
                "--environment": self.env_name,
                "--domain": domain,
                "--table_name": prices_table,
                "--interval": sfn.JsonPath.string_at("$.glue_args['--interval']"),
                "--data_source": sfn.JsonPath.string_at("$.glue_args['--data_source']"),
                "--prices_curated_bucket": self.shared_storage.curated_bucket.bucket_name,
                "--prices_layer": self.curated_layer,
                "--output_bucket": self.shared_storage.curated_bucket.bucket_name,
                "--output_layer": self.indicators_layer,
                "--schema_fingerprint_s3_uri": indicators_fp,
                "--codec": "zstd",
                "--target_file_mb": "256",
                "--lookback_days": str(int(self.config.get("indicators_lookback_days", 252))),
                "--ds.$": "$.ds",
            },
            result_path="$.indicators_glue_args",
        )

        indicators_task = tasks.GlueStartJobRun(
            self,
            "ComputeIndicators",
            glue_job_name=self.indicators_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_json_path_at("$.indicators_glue_args"),
            result_path="$.indicators_etl",
        )
        indicators_task.add_retry(
            errors=["Glue.ConcurrentRunsExceededException"],
            interval=Duration.seconds(self.glue_retry_interval_seconds),
            max_attempts=self.glue_retry_max_attempts,
            backoff_rate=self.glue_retry_backoff_rate,
        )
        indicators_task.add_catch(handler=fail_chain, result_path="$.error")

        decider_fn = self._create_schema_change_decider_function()
        decide_crawler_task = tasks.LambdaInvoke(
            self,
            "DecideCrawler",
            lambda_function=decider_fn,
            payload=sfn.TaskInput.from_object(
                {
                    "glue_args.$": "$.indicators_glue_args",
                    "catalog_update.$": "$.catalog_update",
                }
            ),
            payload_response_only=True,
        )

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

        success_task = sfn.Succeed(
            self,
            "DailyPricesProcessingSuccess",
            comment="Daily prices data processing completed successfully",
        )

        crawler_decision = (
            sfn.Choice(self, "ShouldRunCrawler")
            .when(
                sfn.Condition.boolean_equals("$.shouldRunCrawler", True),
                start_crawler_task.next(success_task),
            )
            .otherwise(success_task)
        )

        processing_sequence = (
            etl_task.next(build_indicators_args).next(indicators_task).next(decide_crawler_task).next(crawler_decision)
        )

        compaction_check_task = tasks.LambdaInvoke(
            self,
            "CheckCompactionOutput",
            lambda_function=compaction_guard_fn,
            payload=sfn.TaskInput.from_object(
                {
                    "bucket.$": "$.glue_args['--compacted_bucket']",
                    "domain": domain,
                    "table_name": prices_table,
                    "interval.$": "$.glue_args['--interval']",
                    "data_source.$": "$.glue_args['--data_source']",
                    "layer": self.compaction_output_subdir,
                    "ds.$": "$.ds",
                }
            ),
            result_path="$.compaction_check",
            payload_response_only=True,
        )
        compaction_check_task.add_catch(handler=fail_chain, result_path="$.error")

        has_compacted_data_choice = (
            sfn.Choice(self, "HasCompactedData")
            .when(
                sfn.Condition.boolean_equals("$.compaction_check.shouldProcess", True),
                processing_sequence,
            )
            .otherwise(success_task)
        )

        preflight_decision = (
            sfn.Choice(self, "PreflightDecision")
            .when(
                sfn.Condition.boolean_equals("$.proceed", True),
                build_compaction_args.next(compaction_task).next(compaction_check_task).next(has_compacted_data_choice),
            )
            .otherwise(
                sfn.Choice(self, "PreflightSkipOrError")
                .when(sfn.Condition.string_equals("$.error.code", "IDEMPOTENT_SKIP"), success_task)
                .when(sfn.Condition.string_equals("$.error.code", "IGNORED_OBJECT"), success_task)
                .otherwise(fail_chain)
            )
        )

        manifest_map = sfn.Map(
            self,
            "ProcessManifestList",
            items_path="$.manifest_keys",
            max_concurrency=self.map_max_concurrency,
            result_path=sfn.JsonPath.DISCARD,
        )
        manifest_map.item_processor(preflight_task.next(preflight_decision))

        all_done = sfn.Succeed(
            self,
            "AllManifestsProcessed",
            comment="All manifests processed sequentially",
        )

        definition = manifest_map.next(all_done)

        sm_log_group = logs.LogGroup(
            self,
            "ProcessingStateMachineLogs",
            retention=self._log_retention(),
        )

        return sfn.StateMachine(
            self,
            "DailyPricesProcessingWorkflow",
            state_machine_name=f"{self.env_name}-daily-prices-data-processing",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            role=iam.Role.from_role_arn(
                self,
                "StepFunctionsExecutionRoleRef",
                self.step_functions_execution_role_arn,
            ),
            logs=sfn.LogOptions(destination=sm_log_group, level=sfn.LogLevel.ALL, include_execution_data=True),
            tracing_enabled=bool(self.config.get("enable_xray_tracing", False)),
            timeout=Duration.hours(2),
        )

    def _create_preflight_function(self) -> lambda_.IFunction:
        """Create preflight Lambda function for ds/idempotency/args."""
        function = PythonFunction(
            self,
            "DailyPricesPreflightFunction",
            function_name=f"{self.env_name}-daily-prices-data-preflight",
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
                "COMPACTION_OUTPUT_SUBDIR": self.compaction_output_subdir,
            },
        )
        return function

    def _create_compaction_guard_function(self) -> lambda_.IFunction:
        """Create Lambda that inspects compacted partitions and reports readiness."""

        return PythonFunction(
            self,
            "CompactionGuardFunction",
            function_name=f"{self.env_name}-daily-prices-compaction-guard",
            runtime=lambda_.Runtime.PYTHON_3_12,
            entry="src/lambda/functions/compaction_guard",
            index="handler.py",
            handler="lambda_handler",
            memory_size=256,
            timeout=Duration.seconds(30),
            log_retention=self._log_retention(),
            role=iam.Role.from_role_arn(self, "CompactionGuardLambdaRole", self.lambda_execution_role_arn),
            layers=[self.common_layer],
            environment={
                "CURATED_BUCKET": self.shared_storage.curated_bucket.bucket_name,
            },
        )

    def _create_schema_change_decider_function(self) -> lambda_.IFunction:
        """Create Lambda to decide if crawler should run based on schema change policy."""
        function = PythonFunction(
            self,
            "SchemaChangeDecider",
            function_name=f"{self.env_name}-schema-change-decider",
            runtime=lambda_.Runtime.PYTHON_3_12,
            entry="src/lambda/functions/schema_change_decider",
            index="handler.py",
            handler="lambda_handler",
            memory_size=256,
            timeout=Duration.seconds(30),
            log_retention=self._log_retention(),
            role=iam.Role.from_role_arn(self, "SchemaDeciderLambdaRole", self.lambda_execution_role_arn),
            layers=[self.common_layer],
            environment={
                "CATALOG_UPDATE_DEFAULT": str(
                    self.config.get(
                        "catalog_update_default",
                        self.config.get("catalog_update", "on_schema_change"),
                    )
                ),
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
            description="Daily prices data ETL job name",
        )

        CfnOutput(
            self,
            "ProcessingWorkflowArn",
            value=self.processing_workflow.state_machine_arn,
            description="Daily prices data processing workflow ARN",
        )

    def _create_common_layer(self) -> lambda_.LayerVersion:
        """Create Common Layer for shared models and utils."""
        return PythonLayerVersion(
            self,
            "CommonLayer",
            entry="src/lambda/layers/common",
            layer_version_name=f"{self.env_name}-common-layer",
            description="Shared common models and utilities",
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            bundling=BundlingOptions(
                command=[
                    "bash",
                    "-c",
                    "set -euxo pipefail; "
                    "mkdir -p /asset-output/python; "
                    "cp -R /asset-input/python/. /asset-output/python/; "
                    "if [ -f requirements.txt ]; then pip install -q -r requirements.txt -t /asset-output/python; fi",
                ],
                asset_excludes=["tests", "__pycache__", "*.pyc"],
            ),
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
