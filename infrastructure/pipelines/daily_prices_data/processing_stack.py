"""Daily prices processing pipeline stack."""

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

        # Feature toggle: processing orchestration (SFN + EB Rule)
        self.enable_processing: bool = bool(self.config.get("enable_processing_orchestration", False))

        if self.enable_processing:
            # Step Functions workflow for orchestration
            self.processing_workflow = self._create_processing_workflow()
            # EventBridge rules: S3 Object Created -> Start State Machine (multi prefix/suffix)
            self.s3_to_sfn_rules = self._create_s3_to_sfn_rules()

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

        domain: str = str(self.config.get("ingestion_domain", "market"))
        table_name: str = str(self.config.get("ingestion_table_name", "prices"))

        raw_prefix = f"{domain}/{table_name}/"
        compacted_prefix = f"{domain}/{table_name}/{self.compaction_output_subdir}"

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
                "--compacted_prefix": compacted_prefix,
                "--codec": self.compaction_codec,
                "--target_file_mb": str(int(self.config.get("compaction_target_file_mb", 256))),
            },
            glue_version="5.0",
            max_retries=1,
            timeout=int(self.config.get("compaction_timeout_minutes", 15)),
            worker_type=str(self.config.get("compaction_worker_type", "G.1X")),
            number_of_workers=int(self.config.get("compaction_number_workers", 2)),
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
        curated_prefix = f"{domain}/{table_name}/adjusted"
        compacted_prefix = f"{domain}/{table_name}/{self.compaction_output_subdir}"

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
                "--enable-s3-parquet-optimized-committer": "1",
                "--codec": "zstd",
                "--target_file_mb": "256",
                "--TempDir": (f"s3://{self.shared_storage.artifacts_bucket.bucket_name}/temp/"),
                # Make the shared Python package available at runtime (provides shared.dq.engine)
                "--extra-py-files": shared_py_asset.s3_object_url,
                "--raw_bucket": self.shared_storage.raw_bucket.bucket_name,
                "--raw_prefix": raw_prefix,
                "--compacted_bucket": self.shared_storage.curated_bucket.bucket_name,
                "--compacted_prefix": compacted_prefix,
                "--curated_bucket": self.shared_storage.curated_bucket.bucket_name,
                "--curated_prefix": curated_prefix,
                "--environment": self.env_name,
                "--schema_fingerprint_s3_uri": schema_fp_uri,
            },
            # Align with docs/spec: Glue 5.0
            glue_version="5.0",
            max_retries=1,  # Spec: 1 retry
            timeout=30,  # Spec: 30 minutes
            worker_type="G.1X",
            number_of_workers=int(self.config.get("glue_max_capacity", 2)),
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

        prices_prefix = f"{domain}/{prices_table}/adjusted"
        indicators_prefix = f"{domain}/{prices_table}/{indicators_table}"

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
                "--enable-s3-parquet-optimized-committer": "1",
                "--codec": "zstd",
                "--target_file_mb": "256",
                "--TempDir": (f"s3://{self.shared_storage.artifacts_bucket.bucket_name}/temp/"),
                "--extra-py-files": f"{shared_py_asset.s3_object_url},{indicators_lib_asset.s3_object_url}",
                # Inputs/outputs
                "--environment": self.env_name,
                "--prices_curated_bucket": self.shared_storage.curated_bucket.bucket_name,
                "--prices_prefix": prices_prefix,
                "--output_bucket": self.shared_storage.curated_bucket.bucket_name,
                "--output_prefix": indicators_prefix,
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
            "PreflightDailyPrices",
            lambda_function=self._create_preflight_function(),
            output_path="$.Payload",
        )

        compaction_guard_fn = self._create_compaction_guard_function()

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

        domain: str = str(self.config.get("ingestion_domain", "market"))
        prices_table: str = str(self.config.get("ingestion_table_name", "prices"))

        build_compaction_args = sfn.Pass(
            self,
            "BuildCompactionArgs",
            parameters={
                "--raw_bucket": sfn.JsonPath.string_at("$.glue_args['--raw_bucket']"),
                "--raw_prefix": sfn.JsonPath.string_at("$.glue_args['--raw_prefix']"),
                "--compacted_bucket": sfn.JsonPath.string_at("$.glue_args['--compacted_bucket']"),
                "--compacted_prefix": sfn.JsonPath.string_at("$.glue_args['--compacted_prefix']"),
                "--file_type": sfn.JsonPath.string_at("$.glue_args['--file_type']"),
                "--interval": sfn.JsonPath.string_at("$.glue_args['--interval']"),
                "--data_source": sfn.JsonPath.string_at("$.glue_args['--data_source']"),
                "--ds": sfn.JsonPath.string_at("$.ds"),
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

        # Prices ETL job task (includes ALL data quality validation)
        etl_task = tasks.GlueStartJobRun(
            self,
            "ProcessDailyPrices",
            glue_job_name=self.etl_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_json_path_at("$.glue_args"),
            result_path="$.prices_etl",
        )
        # Catch Glue task failures (includes DQ failures) and route to normalized fail chain
        etl_task.add_catch(handler=fail_chain, result_path="$.error")

        # Build indicators Glue args from Preflight context and stack constants
        indicators_table: str = str(self.config.get("indicators_table_name", "indicators"))
        indicators_fp = f"s3://{self.shared_storage.artifacts_bucket.bucket_name}/{domain}/{prices_table}/{indicators_table}/_schema/latest.json"

        build_indicators_args = sfn.Pass(
            self,
            "BuildIndicatorsArgs",
            parameters={
                "--environment": self.env_name,
                "--prices_curated_bucket": self.shared_storage.curated_bucket.bucket_name,
                "--prices_prefix": f"{domain}/{prices_table}/adjusted",
                "--output_bucket": self.shared_storage.curated_bucket.bucket_name,
                "--output_prefix": f"{domain}/{prices_table}/{indicators_table}",
                "--schema_fingerprint_s3_uri": indicators_fp,
                "--codec": "zstd",
                "--target_file_mb": "256",
                "--lookback_days": str(int(self.config.get("indicators_lookback_days", 252))),
                "--ds.$": "$.ds",
            },
            result_path="$.indicators_glue_args",
        )

        # Indicators ETL job task
        indicators_task = tasks.GlueStartJobRun(
            self,
            "ComputeIndicators",
            glue_job_name=self.indicators_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_json_path_at("$.indicators_glue_args"),
            result_path="$.indicators_etl",
        )
        indicators_task.add_catch(handler=fail_chain, result_path="$.error")

        # Create schema-change decider Lambda once and reuse across branches
        decider_fn = self._create_schema_change_decider_function()

        # Decide whether to run crawler based on policy + schema change (for indicators)
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
            output_path="$.Payload",
        )

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
                    "prefix.$": "$.glue_args['--compacted_prefix']",
                    "ds.$": "$.ds",
                }
            ),
            result_path="$.compaction_check",
            output_path="$.Payload",
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

        # Create single day processing chain for standalone mode
        # Single day processing: Compaction -> (Transform pipeline?) -> Success
        single_day_processing = (
            sfn.Choice(self, "PreflightChoice")
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

        # Create separate failure states for backfill processing
        backfill_normalize_fail = sfn.Pass(
            self,
            "BackfillNormalizeAndFail",
            parameters={
                "ok": False,
                "error.$": "$.error",
            },
        )
        backfill_fail_state = sfn.Fail(self, "BackfillExecutionFailed", comment="Backfill pipeline execution failed")
        backfill_fail_chain = backfill_normalize_fail.next(backfill_fail_state)

        backfill_build_compaction_args = sfn.Pass(
            self,
            "BackfillBuildCompactionArgs",
            parameters={
                "--raw_bucket": sfn.JsonPath.string_at("$.glue_args['--raw_bucket']"),
                "--raw_prefix": sfn.JsonPath.string_at("$.glue_args['--raw_prefix']"),
                "--compacted_bucket": sfn.JsonPath.string_at("$.glue_args['--compacted_bucket']"),
                "--compacted_prefix": sfn.JsonPath.string_at("$.glue_args['--compacted_prefix']"),
                "--file_type": sfn.JsonPath.string_at("$.glue_args['--file_type']"),
                "--interval": sfn.JsonPath.string_at("$.glue_args['--interval']"),
                "--data_source": sfn.JsonPath.string_at("$.glue_args['--data_source']"),
                "--ds": sfn.JsonPath.string_at("$.ds"),
                "--codec": self.compaction_codec,
                "--target_file_mb": str(int(self.config.get("compaction_target_file_mb", 256))),
            },
            result_path="$.compaction_args",
        )

        backfill_compaction_task = tasks.GlueStartJobRun(
            self,
            "BackfillCompactRawDailyPrices",
            glue_job_name=self.compaction_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_json_path_at("$.compaction_args"),
            result_path=sfn.JsonPath.DISCARD,
        )
        backfill_compaction_task.add_catch(handler=backfill_fail_chain, result_path="$.error")

        backfill_etl_task = tasks.GlueStartJobRun(
            self,
            "BackfillProcessDailyPrices",
            glue_job_name=self.etl_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_json_path_at("$.glue_args"),
            result_path="$.prices_etl",
        )
        backfill_etl_task.add_catch(handler=backfill_fail_chain, result_path="$.error")

        backfill_build_indicators_args = sfn.Pass(
            self,
            "BackfillBuildIndicatorsArgs",
            parameters={
                "--environment": self.env_name,
                "--prices_curated_bucket": self.shared_storage.curated_bucket.bucket_name,
                "--prices_prefix": f"{domain}/{prices_table}/adjusted",
                "--output_bucket": self.shared_storage.curated_bucket.bucket_name,
                "--output_prefix": f"{domain}/{prices_table}/{indicators_table}",
                "--schema_fingerprint_s3_uri": indicators_fp,
                "--codec": "zstd",
                "--target_file_mb": "256",
                "--lookback_days": str(int(self.config.get("indicators_lookback_days", 252))),
                "--ds.$": "$.ds",
            },
            result_path="$.indicators_glue_args",
        )

        backfill_indicators_task = tasks.GlueStartJobRun(
            self,
            "BackfillComputeIndicators",
            glue_job_name=self.indicators_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_json_path_at("$.indicators_glue_args"),
            result_path="$.indicators_etl",
        )
        backfill_indicators_task.add_catch(handler=backfill_fail_chain, result_path="$.error")

        backfill_decide_crawler_task = tasks.LambdaInvoke(
            self,
            "BackfillDecideCrawler",
            lambda_function=decider_fn,
            payload=sfn.TaskInput.from_object(
                {
                    "glue_args.$": "$.indicators_glue_args",
                    "catalog_update.$": "$.catalog_update",
                }
            ),
            output_path="$.Payload",
        )
        backfill_decide_crawler_task.add_catch(handler=backfill_fail_chain, result_path="$.error")

        backfill_start_crawler = tasks.CallAwsService(
            self,
            "BackfillStartCrawler",
            service="glue",
            action="startCrawler",
            parameters={"Name": crawler_name},
            iam_resources=[f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:crawler/{crawler_name}"],
            result_path=sfn.JsonPath.DISCARD,
        )
        backfill_start_crawler.add_catch(handler=backfill_fail_chain, result_path="$.error")

        backfill_success = sfn.Succeed(
            self,
            "BackfillSuccess",
            comment="Backfill processing completed successfully",
        )
        backfill_skip_crawler = sfn.Succeed(
            self,
            "BackfillSkipCrawler",
            comment="Crawler skipped for backfill",
        )

        backfill_crawler_decision = (
            sfn.Choice(self, "BackfillShouldRunCrawler")
            .when(
                sfn.Condition.boolean_equals("$.shouldRunCrawler", True), backfill_start_crawler.next(backfill_success)
            )
            .otherwise(backfill_skip_crawler)
        )

        backfill_processing_sequence = (
            backfill_etl_task.next(backfill_build_indicators_args)
            .next(backfill_indicators_task)
            .next(backfill_decide_crawler_task)
            .next(backfill_crawler_decision)
        )

        backfill_compaction_check = tasks.LambdaInvoke(
            self,
            "BackfillCheckCompactionOutput",
            lambda_function=compaction_guard_fn,
            payload=sfn.TaskInput.from_object(
                {
                    "bucket.$": "$.glue_args['--compacted_bucket']",
                    "prefix.$": "$.glue_args['--compacted_prefix']",
                    "ds.$": "$.ds",
                }
            ),
            result_path="$.compaction_check",
            output_path="$.Payload",
        )
        backfill_compaction_check.add_catch(handler=backfill_fail_chain, result_path="$.error")

        backfill_no_data = sfn.Succeed(
            self,
            "BackfillNoData",
            comment="No compacted output generated for backfill partition",
        )

        backfill_has_data_choice = (
            sfn.Choice(self, "BackfillHasCompactedData")
            .when(
                sfn.Condition.boolean_equals("$.compaction_check.shouldProcess", True),
                backfill_processing_sequence,
            )
            .otherwise(backfill_no_data)
        )

        # Create separate processing chain for backfill map iterator (avoid state reuse)
        backfill_processing = (
            sfn.Choice(self, "BackfillPreflightChoice")
            .when(
                sfn.Condition.boolean_equals("$.proceed", True),
                backfill_build_compaction_args.next(backfill_compaction_task)
                .next(backfill_compaction_check)
                .next(backfill_has_data_choice),
            )
            .otherwise(
                sfn.Choice(self, "BackfillPreflightSkipOrError")
                .when(
                    sfn.Condition.string_equals("$.error.code", "IDEMPOTENT_SKIP"),
                    sfn.Succeed(self, "BackfillSkipSuccess", comment="Processing skipped (idempotent)"),
                )
                .when(
                    sfn.Condition.string_equals("$.error.code", "IGNORED_OBJECT"),
                    sfn.Succeed(self, "BackfillSkipNonMatching", comment="Processing skipped (ignored object)"),
                )
                .otherwise(backfill_fail_chain)
            )
        )

        # Create backfill map state for concurrent processing
        backfill_map = sfn.Map(
            self,
            "BackfillMap",
            max_concurrency=3,
            items_path="$.dates",
        )
        # Use the modern itemProcessor API instead of deprecated iterator
        backfill_map.item_processor(backfill_processing)

        # Main workflow: supports both single day and backfill modes
        definition = preflight_task.next(
            sfn.Choice(self, "BackfillOrSingle")
            .when(sfn.Condition.is_present("$.dates"), backfill_map)
            .otherwise(single_day_processing)
        )
        top_definition = definition

        # Create log group for the state machine with retention from config
        sm_log_group = logs.LogGroup(
            self,
            "ProcessingStateMachineLogs",
            retention=self._log_retention(),
        )

        return sfn.StateMachine(
            self,
            "DailyPricesProcessingWorkflow",
            state_machine_name=f"{self.env_name}-daily-prices-data-processing",
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
                    "suffixes": self.config.get("processing_suffixes", [".manifest.json"]),
                }
            ]

        created: list[events.Rule] = []
        for t in triggers:
            domain: str = str(t.get("domain", "")).strip()
            table_name: str = str(t.get("table_name", "")).strip()
            file_type: str = str(t.get("file_type", "json")).strip() or "json"
            suffixes: list[str] = list(t.get("suffixes", [".manifest.json"]))

            if not domain or not table_name:
                # Skip malformed trigger
                continue

            rule = self._add_s3_to_sfn_rule(domain, table_name, file_type, suffixes)
            created.append(rule)

        return created

    def _add_s3_to_sfn_rule(self, domain: str, table_name: str, file_type: str, suffixes: list[str]) -> events.Rule:
        """Create a single EventBridge rule for a specific domain/table and suffix set."""
        prefix = f"{domain}/{table_name}/"

        # Normalize suffix list; empty values fall back to prefix-only filtering.
        sanitized_suffixes = [str(s).strip() for s in suffixes if str(s).strip()]

        rule_id = f"RawObjectCreated-{domain}-{table_name}".replace("/", "-")
        # EventBridge now filters by both prefix and suffix, ensuring only manifest
        # files (or other explicitly allowed suffixes) invoke the state machine.
        key_patterns: list[dict[str, str]]
        if sanitized_suffixes:
            key_patterns = [{"prefix": prefix, "suffix": suffix} for suffix in sanitized_suffixes]
        else:
            key_patterns = [{"prefix": prefix}]

        rule = events.Rule(
            self,
            rule_id,
            rule_name=f"{self.env_name}-raw-object-created-{domain}-{table_name}",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [self.shared_storage.raw_bucket.bucket_name]},
                    "object": {"key": key_patterns},
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
                        "allowed_suffixes": sanitized_suffixes,
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

        # Output only when processing is enabled
        if getattr(self, "enable_processing", False) and hasattr(self, "processing_workflow"):
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
