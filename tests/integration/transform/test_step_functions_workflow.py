"""
Step Functions Workflow Integration Tests

Transform 도메인의 Step Functions 상태머신 통합 테스트입니다.
실제 Step Functions 실행을 통해 워크플로우 상태 전이, 에러 핸들링,
백필 맵 처리를 검증합니다.

테스트 범위:
- 단일 파티션 처리 워크플로우
- 백필 맵 동시성 처리
- 에러 상황별 상태 전이
- Preflight → Glue → Crawler 체인 검증
"""

import pytest
import boto3
import json
import time
from moto import mock_stepfunctions, mock_lambda, mock_s3, mock_glue
from unittest.mock import patch, MagicMock


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    import os

    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def sfn_definition():
    """Simplified Step Functions definition for testing."""
    return {
        "Comment": "Transform Pipeline Test Workflow",
        "StartAt": "Preflight",
        "States": {
            "Preflight": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {"FunctionName": "test-preflight-function", "Payload.$": "$"},
                "ResultPath": "$.preflight_result",
                "Next": "CheckProceed",
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "HandleError", "ResultPath": "$.error"}],
            },
            "CheckProceed": {
                "Type": "Choice",
                "Choices": [
                    {"Variable": "$.preflight_result.Payload.proceed", "BooleanEquals": True, "Next": "GlueETL"},
                    {
                        "Variable": "$.preflight_result.Payload.error.code",
                        "StringEquals": "IDEMPOTENT_SKIP",
                        "Next": "Success",
                    },
                ],
                "Default": "HandleError",
            },
            "GlueETL": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": "test-customer-data-etl",
                    "Arguments.$": "$.preflight_result.Payload.glue_args",
                },
                "ResultPath": "$.glue_result",
                "Next": "StartCrawler",
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "HandleError", "ResultPath": "$.error"}],
            },
            "StartCrawler": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
                "Parameters": {"Name": "test-curated-data-crawler"},
                "ResultPath": "$.crawler_result",
                "Next": "Success",
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "HandleError", "ResultPath": "$.error"}],
            },
            "Success": {"Type": "Succeed", "Comment": "Pipeline completed successfully"},
            "HandleError": {"Type": "Pass", "Parameters": {"ok": False, "error.$": "$.error"}, "Next": "Fail"},
            "Fail": {"Type": "Fail", "Comment": "Pipeline execution failed"},
        },
    }


@pytest.fixture
def backfill_sfn_definition():
    """Backfill Map workflow definition for testing."""
    return {
        "Comment": "Transform Backfill Map Test Workflow",
        "StartAt": "Preflight",
        "States": {
            "Preflight": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {"FunctionName": "test-preflight-function", "Payload.$": "$"},
                "ResultPath": "$.preflight_result",
                "Next": "CheckBackfill",
            },
            "CheckBackfill": {
                "Type": "Choice",
                "Choices": [{"Variable": "$.dates", "IsPresent": True, "Next": "BackfillMap"}],
                "Default": "SingleDayProcessing",
            },
            "BackfillMap": {
                "Type": "Map",
                "ItemsPath": "$.dates",
                "MaxConcurrency": 3,
                "ItemProcessor": {
                    "ProcessorConfig": {"Mode": "INLINE"},
                    "StartAt": "ProcessPartition",
                    "States": {
                        "ProcessPartition": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::glue:startJobRun.sync",
                            "Parameters": {"JobName": "test-customer-data-etl", "Arguments": {"--ds.$": "$"}},
                            "Next": "PartitionSuccess",
                        },
                        "PartitionSuccess": {"Type": "Succeed"},
                    },
                },
                "Next": "BackfillSuccess",
            },
            "SingleDayProcessing": {"Type": "Succeed", "Comment": "Single day processing completed"},
            "BackfillSuccess": {"Type": "Succeed", "Comment": "Backfill completed successfully"},
        },
    }


@pytest.mark.integration
class TestStepFunctionsWorkflow:
    """Step Functions 워크플로우 통합 테스트 클래스"""

    @mock_stepfunctions
    @mock_lambda
    @mock_s3
    @mock_glue
    def test_single_partition_workflow_success(self, aws_credentials, sfn_definition):
        """
        Given: 단일 파티션 처리를 위한 정상적인 입력
        When: Step Functions 워크플로우를 실행하면
        Then: Preflight → Glue → Crawler 순서로 성공적으로 실행되어야 함
        """
        # Setup mocks
        sfn_client = boto3.client("stepfunctions", region_name="us-east-1")
        lambda_client = boto3.client("lambda", region_name="us-east-1")
        glue_client = boto3.client("glue", region_name="us-east-1")

        # Create Lambda function for Preflight
        lambda_client.create_function(
            FunctionName="test-preflight-function",
            Runtime="python3.12",
            Role="arn:aws:iam::123456789012:role/test-role",
            Handler="handler.lambda_handler",
            Code={"ZipFile": b"fake code"},
        )

        # Create Glue job
        glue_client.create_job(
            Name="test-customer-data-etl",
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://test/script.py"},
        )

        # Create Glue crawler
        glue_client.create_crawler(
            Name="test-curated-data-crawler",
            Role="arn:aws:iam::123456789012:role/crawler-role",
            Targets={"S3Targets": [{"Path": "s3://test-bucket/curated/"}]},
        )

        # Create state machine
        sm_arn = sfn_client.create_state_machine(
            name="test-transform-workflow",
            definition=json.dumps(sfn_definition),
            roleArn="arn:aws:iam::123456789012:role/stepfunctions-role",
        )["stateMachineArn"]

        # Mock successful responses
        with patch("boto3.client") as mock_boto:
            mock_lambda_client = MagicMock()
            mock_glue_client = MagicMock()

            # Configure return values based on service
            def side_effect(service, **kwargs):
                if service == "lambda":
                    return mock_lambda_client
                elif service == "glue":
                    return mock_glue_client
                else:
                    return boto3.client(service, **kwargs)

            mock_boto.side_effect = side_effect

            # Mock Preflight success response
            mock_lambda_client.invoke.return_value = {
                "StatusCode": 200,
                "Payload": MagicMock(
                    read=lambda: json.dumps(
                        {
                            "proceed": True,
                            "ds": "2025-09-07",
                            "glue_args": {
                                "--ds": "2025-09-07",
                                "--raw_bucket": "test-raw",
                                "--curated_bucket": "test-curated",
                            },
                        }
                    ).encode()
                ),
            }

            # Mock Glue job success
            mock_glue_client.start_job_run.return_value = {"JobRunId": "jr_test123"}
            mock_glue_client.get_job_run.return_value = {"JobRun": {"JobRunState": "SUCCEEDED"}}

            # Mock Crawler success
            mock_glue_client.start_crawler.return_value = {}

            # Execute workflow
            execution_input = {
                "source_bucket": "test-raw-bucket",
                "source_key": "market/prices/ingestion_date=2025-09-07/file.json",
                "domain": "market",
                "table_name": "prices",
                "file_type": "json",
            }

            execution_arn = sfn_client.start_execution(
                stateMachineArn=sm_arn,
                name="test-execution-success",
                input=json.dumps(execution_input),
            )["executionArn"]

            # Wait for completion (mocked, so immediate)
            time.sleep(0.1)

            # Verify execution completed successfully
            execution = sfn_client.describe_execution(executionArn=execution_arn)
            assert execution["status"] == "SUCCEEDED"

            # Verify service calls were made
            mock_lambda_client.invoke.assert_called_once()
            mock_glue_client.start_job_run.assert_called_once()
            mock_glue_client.start_crawler.assert_called_once()

    @mock_stepfunctions
    @mock_lambda
    def test_preflight_idempotent_skip_workflow(self, aws_credentials, sfn_definition):
        """
        Given: 이미 처리된 파티션 (멱등성 스킵)
        When: Step Functions 워크플로우를 실행하면
        Then: Preflight에서 IDEMPOTENT_SKIP 후 성공으로 종료되어야 함
        """
        sfn_client = boto3.client("stepfunctions", region_name="us-east-1")
        lambda_client = boto3.client("lambda", region_name="us-east-1")

        # Create Lambda function
        lambda_client.create_function(
            FunctionName="test-preflight-function",
            Runtime="python3.12",
            Role="arn:aws:iam::123456789012:role/test-role",
            Handler="handler.lambda_handler",
            Code={"ZipFile": b"fake code"},
        )

        # Create state machine
        sm_arn = sfn_client.create_state_machine(
            name="test-idempotent-workflow",
            definition=json.dumps(sfn_definition),
            roleArn="arn:aws:iam::123456789012:role/stepfunctions-role",
        )["stateMachineArn"]

        # Mock idempotent skip response
        with patch("boto3.client") as mock_boto:
            mock_lambda_client = MagicMock()
            mock_boto.return_value = mock_lambda_client

            # Mock Preflight idempotent skip response
            mock_lambda_client.invoke.return_value = {
                "StatusCode": 200,
                "Payload": MagicMock(
                    read=lambda: json.dumps(
                        {
                            "proceed": False,
                            "ds": "2025-09-07",
                            "error": {"code": "IDEMPOTENT_SKIP", "message": "Partition already processed"},
                        }
                    ).encode()
                ),
            }

            # Execute workflow
            execution_input = {
                "source_bucket": "test-raw-bucket",
                "source_key": "market/prices/ingestion_date=2025-09-07/file.json",
                "domain": "market",
                "table_name": "prices",
                "file_type": "json",
            }

            execution_arn = sfn_client.start_execution(
                stateMachineArn=sm_arn,
                name="test-execution-idempotent",
                input=json.dumps(execution_input),
            )["executionArn"]

            # Wait for completion
            time.sleep(0.1)

            # Verify execution completed successfully (idempotent skip is success)
            execution = sfn_client.describe_execution(executionArn=execution_arn)
            assert execution["status"] == "SUCCEEDED"

            # Verify only Preflight was called (no Glue/Crawler)
            mock_lambda_client.invoke.assert_called_once()

    @mock_stepfunctions
    @mock_lambda
    @mock_glue
    def test_glue_job_failure_workflow(self, aws_credentials, sfn_definition):
        """
        Given: Glue ETL job이 실패하는 상황
        When: Step Functions 워크플로우를 실행하면
        Then: Glue 실패 후 에러 핸들링 상태로 전이되어야 함
        """
        sfn_client = boto3.client("stepfunctions", region_name="us-east-1")
        lambda_client = boto3.client("lambda", region_name="us-east-1")
        glue_client = boto3.client("glue", region_name="us-east-1")

        # Setup resources
        lambda_client.create_function(
            FunctionName="test-preflight-function",
            Runtime="python3.12",
            Role="arn:aws:iam::123456789012:role/test-role",
            Handler="handler.lambda_handler",
            Code={"ZipFile": b"fake code"},
        )

        glue_client.create_job(
            Name="test-customer-data-etl",
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://test/script.py"},
        )

        sm_arn = sfn_client.create_state_machine(
            name="test-glue-failure-workflow",
            definition=json.dumps(sfn_definition),
            roleArn="arn:aws:iam::123456789012:role/stepfunctions-role",
        )["stateMachineArn"]

        # Mock responses with Glue failure
        with patch("boto3.client") as mock_boto:
            mock_lambda_client = MagicMock()
            mock_glue_client = MagicMock()

            def side_effect(service, **kwargs):
                if service == "lambda":
                    return mock_lambda_client
                elif service == "glue":
                    return mock_glue_client
                else:
                    return boto3.client(service, **kwargs)

            mock_boto.side_effect = side_effect

            # Mock successful Preflight
            mock_lambda_client.invoke.return_value = {
                "StatusCode": 200,
                "Payload": MagicMock(
                    read=lambda: json.dumps(
                        {"proceed": True, "ds": "2025-09-07", "glue_args": {"--ds": "2025-09-07"}}
                    ).encode()
                ),
            }

            # Mock Glue job failure
            mock_glue_client.start_job_run.return_value = {"JobRunId": "jr_test123"}
            mock_glue_client.get_job_run.return_value = {
                "JobRun": {"JobRunState": "FAILED", "ErrorMessage": "DQ_FAILED: null ratio exceeded for column price"}
            }

            # Execute workflow
            execution_input = {
                "source_bucket": "test-raw-bucket",
                "source_key": "market/prices/ingestion_date=2025-09-07/file.json",
                "domain": "market",
                "table_name": "prices",
                "file_type": "json",
            }

            execution_arn = sfn_client.start_execution(
                stateMachineArn=sm_arn,
                name="test-execution-glue-failure",
                input=json.dumps(execution_input),
            )["executionArn"]

            # Wait for completion
            time.sleep(0.1)

            # Verify execution failed due to Glue error
            execution = sfn_client.describe_execution(executionArn=execution_arn)
            assert execution["status"] == "FAILED"

            # Verify Glue was attempted
            mock_glue_client.start_job_run.assert_called_once()

    @mock_stepfunctions
    @mock_lambda
    @mock_glue
    def test_backfill_map_workflow(self, aws_credentials, backfill_sfn_definition):
        """
        Given: 백필 처리를 위한 날짜 배열 입력
        When: Step Functions Map 워크플로우를 실행하면
        Then: 각 날짜별로 병렬 처리되어야 함 (MaxConcurrency=3)
        """
        sfn_client = boto3.client("stepfunctions", region_name="us-east-1")
        lambda_client = boto3.client("lambda", region_name="us-east-1")
        glue_client = boto3.client("glue", region_name="us-east-1")

        # Setup resources
        lambda_client.create_function(
            FunctionName="test-preflight-function",
            Runtime="python3.12",
            Role="arn:aws:iam::123456789012:role/test-role",
            Handler="handler.lambda_handler",
            Code={"ZipFile": b"fake code"},
        )

        glue_client.create_job(
            Name="test-customer-data-etl",
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://test/script.py"},
        )

        sm_arn = sfn_client.create_state_machine(
            name="test-backfill-workflow",
            definition=json.dumps(backfill_sfn_definition),
            roleArn="arn:aws:iam::123456789012:role/stepfunctions-role",
        )["stateMachineArn"]

        # Mock successful responses
        with patch("boto3.client") as mock_boto:
            mock_lambda_client = MagicMock()
            mock_glue_client = MagicMock()

            def side_effect(service, **kwargs):
                if service == "lambda":
                    return mock_lambda_client
                elif service == "glue":
                    return mock_glue_client
                else:
                    return boto3.client(service, **kwargs)

            mock_boto.side_effect = side_effect

            # Mock Preflight success
            mock_lambda_client.invoke.return_value = {
                "StatusCode": 200,
                "Payload": MagicMock(
                    read=lambda: json.dumps(
                        {"proceed": True, "dates": ["2025-09-05", "2025-09-06", "2025-09-07"]}
                    ).encode()
                ),
            }

            # Mock Glue success for each partition
            mock_glue_client.start_job_run.return_value = {"JobRunId": "jr_test123"}
            mock_glue_client.get_job_run.return_value = {"JobRun": {"JobRunState": "SUCCEEDED"}}

            # Execute backfill workflow
            execution_input = {
                "domain": "market",
                "table_name": "prices",
                "dates": ["2025-09-05", "2025-09-06", "2025-09-07"],
            }

            execution_arn = sfn_client.start_execution(
                stateMachineArn=sm_arn,
                name="test-execution-backfill",
                input=json.dumps(execution_input),
            )["executionArn"]

            # Wait for completion
            time.sleep(0.1)

            # Verify execution completed successfully
            execution = sfn_client.describe_execution(executionArn=execution_arn)
            assert execution["status"] == "SUCCEEDED"

            # Verify Glue was called for each partition (3 times)
            assert mock_glue_client.start_job_run.call_count == 3

    @mock_stepfunctions
    @mock_lambda
    def test_preflight_validation_failure_workflow(self, aws_credentials, sfn_definition):
        """
        Given: 잘못된 입력으로 인한 Preflight 검증 실패
        When: Step Functions 워크플로우를 실행하면
        Then: PRE_VALIDATION_FAILED 에러로 워크플로우가 실패해야 함
        """
        sfn_client = boto3.client("stepfunctions", region_name="us-east-1")
        lambda_client = boto3.client("lambda", region_name="us-east-1")

        # Setup Lambda
        lambda_client.create_function(
            FunctionName="test-preflight-function",
            Runtime="python3.12",
            Role="arn:aws:iam::123456789012:role/test-role",
            Handler="handler.lambda_handler",
            Code={"ZipFile": b"fake code"},
        )

        sm_arn = sfn_client.create_state_machine(
            name="test-validation-failure-workflow",
            definition=json.dumps(sfn_definition),
            roleArn="arn:aws:iam::123456789012:role/stepfunctions-role",
        )["stateMachineArn"]

        # Mock validation failure response
        with patch("boto3.client") as mock_boto:
            mock_lambda_client = MagicMock()
            mock_boto.return_value = mock_lambda_client

            mock_lambda_client.invoke.return_value = {
                "StatusCode": 200,
                "Payload": MagicMock(
                    read=lambda: json.dumps(
                        {
                            "proceed": False,
                            "error": {"code": "PRE_VALIDATION_FAILED", "message": "Missing required field: domain"},
                        }
                    ).encode()
                ),
            }

            # Execute with invalid input (missing domain)
            execution_input = {
                "source_bucket": "test-raw-bucket",
                "source_key": "market/prices/ingestion_date=2025-09-07/file.json",
                "table_name": "prices",
                "file_type": "json",
                # Missing "domain" field
            }

            execution_arn = sfn_client.start_execution(
                stateMachineArn=sm_arn,
                name="test-execution-validation-failure",
                input=json.dumps(execution_input),
            )["executionArn"]

            # Wait for completion
            time.sleep(0.1)

            # Verify execution failed due to validation error
            execution = sfn_client.describe_execution(executionArn=execution_arn)
            assert execution["status"] == "FAILED"

            # Verify only Preflight was called
            mock_lambda_client.invoke.assert_called_once()
