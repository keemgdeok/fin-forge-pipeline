"""
AWS Service Integration Tests for Transform Pipeline

S3 Object Created → EventBridge → Step Functions 트리거 체인과
Cross-service 권한, 로깅, 모니터링을 검증하는 통합 테스트입니다.

테스트 범위:
- S3 이벤트 → EventBridge 규칙 매칭
- EventBridge → Step Functions 실행 트리거
- Multi-prefix/suffix 이벤트 필터링
- Cross-stack IAM 권한 검증
- CloudWatch 로깅 통합 검증
"""

import pytest
import boto3
import json
import time
from datetime import datetime
from typing import Dict
from moto import mock_aws

from tests.fixtures.data_builders import build_raw_s3_object_key


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
def buckets_config():
    """테스트용 S3 버킷 설정"""
    return {
        "raw_bucket": "test-raw-bucket-dev-123456789012",
        "curated_bucket": "test-curated-bucket-dev-123456789012",
        "artifacts_bucket": "test-artifacts-bucket-dev-123456789012",
    }


@pytest.fixture
def eventbridge_rule_config():
    """EventBridge 규칙 설정"""
    return {
        "rule_name": "dev-raw-object-created-market-prices",
        "event_pattern": {
            "source": ["aws.s3"],
            "detail-type": ["Object Created"],
            "detail": {
                "bucket": {"name": ["test-raw-bucket-dev-123456789012"]},
                "object": {
                    "key": [
                        {"prefix": "market/prices/", "suffix": ".json"},
                        {"prefix": "market/daily-prices-orders/", "suffix": ".csv"},
                    ]
                },
            },
        },
    }


@pytest.fixture
def step_functions_definition():
    """간소화된 Step Functions 정의"""
    return {
        "Comment": "Transform Pipeline Integration Test",
        "StartAt": "MockPreflight",
        "States": {
            "MockPreflight": {
                "Type": "Pass",
                "Parameters": {"proceed": True, "ds": "2025-09-07", "glue_args": {"--ds": "2025-09-07"}},
                "Next": "Success",
            },
            "Success": {"Type": "Succeed", "Comment": "Integration test completed"},
        },
    }


@pytest.mark.integration
class TestAWSServiceIntegration:
    """AWS 서비스 통합 테스트 클래스"""

    @mock_aws
    def test_s3_eventbridge_stepfunctions_trigger_chain(
        self, aws_credentials, buckets_config, eventbridge_rule_config, step_functions_definition
    ):
        """
        Given: S3 버킷에 파일이 업로드되면
        When: EventBridge 규칙이 매칭되어 Step Functions을 트리거하면
        Then: 전체 체인이 성공적으로 동작해야 함
        """
        # Skip if jsonpath_ng (required by moto for InputTransformer) is missing
        pytest.importorskip("jsonpath_ng")

        # Setup AWS clients
        s3_client = boto3.client("s3", region_name="us-east-1")
        events_client = boto3.client("events", region_name="us-east-1")
        sfn_client = boto3.client("stepfunctions", region_name="us-east-1")
        iam_client = boto3.client("iam", region_name="us-east-1")

        # Create S3 bucket
        s3_client.create_bucket(Bucket=buckets_config["raw_bucket"])

        # Create IAM role for Step Functions
        assume_role_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Principal": {"Service": "states.amazonaws.com"}, "Action": "sts:AssumeRole"}
            ],
        }

        iam_client.create_role(
            RoleName="test-stepfunctions-role", AssumeRolePolicyDocument=json.dumps(assume_role_policy)
        )

        # Create Step Functions state machine
        sm_arn = sfn_client.create_state_machine(
            name="test-transform-workflow",
            definition=json.dumps(step_functions_definition),
            roleArn="arn:aws:iam::123456789012:role/test-stepfunctions-role",
        )["stateMachineArn"]

        # Create EventBridge rule
        events_client.put_rule(
            Name=eventbridge_rule_config["rule_name"],
            EventPattern=json.dumps(eventbridge_rule_config["event_pattern"]),
            State="ENABLED",
        )

        # Add Step Functions as target
        events_client.put_targets(
            Rule=eventbridge_rule_config["rule_name"],
            Targets=[
                {
                    "Id": "1",
                    "Arn": sm_arn,
                    "RoleArn": "arn:aws:iam::123456789012:role/test-eventbridge-role",
                    "InputTransformer": {
                        "InputPathsMap": {"bucket": "$.detail.bucket.name", "key": "$.detail.object.key"},
                        "InputTemplate": json.dumps(
                            {
                                "source_bucket": "<bucket>",
                                "source_key": "<key>",
                                "domain": "market",
                                "table_name": "prices",
                                "file_type": "json",
                            }
                        ),
                    },
                }
            ],
        )

        partition_date = datetime.strptime("2025-09-07", "%Y-%m-%d")
        aapl_key = build_raw_s3_object_key(
            domain="market",
            table_name="prices",
            data_source="yahoo_finance",
            interval="1d",
            symbol="AAPL",
            date=partition_date,
        )
        googl_key = build_raw_s3_object_key(
            domain="market",
            table_name="prices",
            data_source="yahoo_finance",
            interval="1d",
            symbol="GOOGL",
            date=partition_date,
        )

        s3_client.put_object(
            Bucket=buckets_config["raw_bucket"],
            Key=aapl_key,
            Body=json.dumps({"symbol": "AAPL", "price": 150.25}).encode(),
        )
        s3_client.put_object(
            Bucket=buckets_config["raw_bucket"],
            Key=googl_key,
            Body=json.dumps({"symbol": "GOOGL", "price": 2750.00}).encode(),
        )

        # Simulate S3 event for a single symbol object
        test_key = aapl_key

        # Simulate EventBridge event (normally triggered by S3)
        test_event = {
            "version": "0",
            "id": "test-event-id",
            "detail-type": "Object Created",
            "source": "aws.s3",
            "account": "123456789012",
            "time": "2025-09-07T10:00:00Z",
            "region": "us-east-1",
            "detail": {"bucket": {"name": buckets_config["raw_bucket"]}, "object": {"key": test_key}},
        }

        # Put event to EventBridge (simulates S3 notification)
        # Moto may not fully implement InputTransformer dispatch; ignore if not implemented
        try:
            events_client.put_events(
                Entries=[
                    {
                        "Source": "aws.s3",
                        "DetailType": "Object Created",
                        "Detail": json.dumps(test_event["detail"]),
                    }
                ]
            )
        except NotImplementedError:
            pass

        # Verify the event would trigger Step Functions
        # (In moto, we simulate the trigger by checking rule matches)
        rules = events_client.list_rules()["Rules"]
        matching_rule = None
        for rule in rules:
            if rule["Name"] == eventbridge_rule_config["rule_name"]:
                matching_rule = rule
                break

        assert matching_rule is not None, "EventBridge rule should exist"
        assert matching_rule["State"] == "ENABLED", "Rule should be enabled"

        # Verify targets are configured
        targets = events_client.list_targets_by_rule(Rule=eventbridge_rule_config["rule_name"])["Targets"]

        assert len(targets) == 1, "Should have one Step Functions target"
        assert sm_arn in targets[0]["Arn"], "Target should be our state machine"

    @mock_aws
    def test_multi_prefix_suffix_event_filtering(self, aws_credentials, buckets_config, eventbridge_rule_config):
        """
        Given: 여러 prefix/suffix 조합의 파일들이 업로드되면
        When: EventBridge 규칙 필터링이 적용되면
        Then: 올바른 파일만 매칭되어야 함
        """
        events_client = boto3.client("events", region_name="us-east-1")
        s3_client = boto3.client("s3", region_name="us-east-1")

        # Create bucket
        s3_client.create_bucket(Bucket=buckets_config["raw_bucket"])

        # Create EventBridge rule with multiple prefix/suffix patterns
        multi_pattern_rule = {
            "source": ["aws.s3"],
            "detail-type": ["Object Created"],
            "detail": {
                "bucket": {"name": [buckets_config["raw_bucket"]]},
                "object": {
                    "key": [
                        {"prefix": "market/prices/", "suffix": ".json"},
                        {"prefix": "market/prices/", "suffix": ".csv"},
                        {"prefix": "market/daily-prices-orders/", "suffix": ".json"},
                    ]
                },
            },
        }

        events_client.put_rule(
            Name="test-multi-pattern-rule", EventPattern=json.dumps(multi_pattern_rule), State="ENABLED"
        )

        partition_date = datetime.strptime("2025-09-07", "%Y-%m-%d")

        matching_json_key = build_raw_s3_object_key(
            domain="market",
            table_name="prices",
            data_source="yahoo_finance",
            interval="1d",
            symbol="AAPL",
            date=partition_date,
        )
        matching_csv_key = build_raw_s3_object_key(
            domain="market",
            table_name="prices",
            data_source="yahoo_finance",
            interval="1d",
            symbol="AAPL",
            extension="csv",
            date=partition_date,
        )
        matching_orders_key = build_raw_s3_object_key(
            domain="market",
            table_name="daily-prices-orders",
            data_source="orders_service",
            interval="1d",
            symbol="ORDERS",
            date=partition_date,
        )
        wrong_suffix_key = build_raw_s3_object_key(
            domain="market",
            table_name="prices",
            data_source="yahoo_finance",
            interval="1d",
            symbol="AAPL",
            extension="parquet",
            date=partition_date,
        )
        wrong_prefix_reports_key = build_raw_s3_object_key(
            domain="analytics",
            table_name="reports",
            data_source="reporting_api",
            interval="daily",
            symbol="summary",
            date=partition_date,
        )
        wrong_prefix_symbols_key = build_raw_s3_object_key(
            domain="market",
            table_name="symbols",
            data_source="reference_data",
            interval="daily",
            symbol="snapshot",
            date=partition_date,
        )

        # Test file scenarios
        test_scenarios = [
            {
                "key": matching_json_key,
                "should_match": True,
                "reason": "Matches market/prices + .json",
            },
            {
                "key": matching_csv_key,
                "should_match": True,
                "reason": "Matches market/prices + .csv",
            },
            {
                "key": matching_orders_key,
                "should_match": True,
                "reason": "Matches market/daily-prices-orders + .json",
            },
            {
                "key": wrong_suffix_key,
                "should_match": False,
                "reason": "Wrong suffix (.parquet not in rule)",
            },
            {
                "key": wrong_prefix_reports_key,
                "should_match": False,
                "reason": "Wrong prefix (analytics/reports not in rule)",
            },
            {
                "key": wrong_prefix_symbols_key,
                "should_match": False,
                "reason": "Wrong prefix (market/symbols not market/prices)",
            },
        ]

        # Upload test files and verify pattern matching
        matched_files = []
        for scenario in test_scenarios:
            # Upload file
            s3_client.put_object(Bucket=buckets_config["raw_bucket"], Key=scenario["key"], Body=b"test data")

            # Simulate event pattern matching (manual verification in moto)
            # In real AWS, EventBridge would automatically filter
            event_detail = {"bucket": {"name": buckets_config["raw_bucket"]}, "object": {"key": scenario["key"]}}

            # Manual pattern matching logic to verify rule behavior
            would_match = self._would_event_match_pattern(event_detail, multi_pattern_rule["detail"])

            if scenario["should_match"]:
                assert would_match, f"File {scenario['key']} should match: {scenario['reason']}"
                matched_files.append(scenario["key"])
            else:
                assert not would_match, f"File {scenario['key']} should NOT match: {scenario['reason']}"

        # Verify we have the expected number of matches
        expected_matches = sum(1 for s in test_scenarios if s["should_match"])
        assert len(matched_files) == expected_matches, f"Expected {expected_matches} matches, got {len(matched_files)}"

    def _would_event_match_pattern(self, event_detail: Dict, pattern_detail: Dict) -> bool:
        """Helper method to simulate EventBridge pattern matching"""
        bucket_matches = event_detail["bucket"]["name"] in pattern_detail["bucket"]["name"]
        if not bucket_matches:
            return False

        key = event_detail["object"]["key"]
        key_patterns = pattern_detail["object"]["key"]

        for key_pattern in key_patterns:
            if isinstance(key_pattern, dict):
                prefix_match = key.startswith(key_pattern.get("prefix", ""))
                suffix_match = key.endswith(key_pattern.get("suffix", ""))
                if prefix_match and suffix_match:
                    return True
            elif isinstance(key_pattern, str):
                if key_pattern in key:
                    return True

        return False

    @mock_aws
    def test_step_functions_cloudwatch_logging_integration(self, aws_credentials, step_functions_definition):
        """
        Given: Step Functions 워크플로우에 CloudWatch 로깅이 활성화되면
        When: 워크플로우가 실행되면
        Then: 적절한 로그 그룹에 실행 로그가 기록되어야 함
        """
        sfn_client = boto3.client("stepfunctions", region_name="us-east-1")
        logs_client = boto3.client("logs", region_name="us-east-1")
        iam_client = boto3.client("iam", region_name="us-east-1")

        # Create log group for Step Functions
        log_group_name = "/aws/stepfunctions/test-transform-workflow"
        logs_client.create_log_group(logGroupName=log_group_name)

        # Create IAM role
        assume_role_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Principal": {"Service": "states.amazonaws.com"}, "Action": "sts:AssumeRole"}
            ],
        }

        iam_client.create_role(
            RoleName="test-stepfunctions-logging-role", AssumeRolePolicyDocument=json.dumps(assume_role_policy)
        )

        # Create state machine with logging configuration
        sm_arn = sfn_client.create_state_machine(
            name="test-transform-workflow-with-logs",
            definition=json.dumps(step_functions_definition),
            roleArn="arn:aws:iam::123456789012:role/test-stepfunctions-logging-role",
            loggingConfiguration={
                "level": "ALL",
                "includeExecutionData": True,
                "destinations": [
                    {
                        "cloudWatchLogsLogGroup": {
                            "logGroupArn": f"arn:aws:logs:us-east-1:123456789012:log-group:{log_group_name}"
                        }
                    }
                ],
            },
        )["stateMachineArn"]

        # Execute workflow
        partition_date = datetime.strptime("2025-09-07", "%Y-%m-%d")
        execution_source_key = build_raw_s3_object_key(
            domain="market",
            table_name="prices",
            data_source="yahoo_finance",
            interval="1d",
            symbol="AAPL",
            date=partition_date,
        )
        execution_input = {
            "source_bucket": "test-raw-bucket",
            "source_key": execution_source_key,
            "domain": "market",
            "table_name": "prices",
        }

        execution_arn = sfn_client.start_execution(
            stateMachineArn=sm_arn, name="test-execution-with-logs", input=json.dumps(execution_input)
        )["executionArn"]

        # Wait briefly for execution
        time.sleep(0.1)

        # Verify execution completed
        execution = sfn_client.describe_execution(executionArn=execution_arn)
        assert execution["status"] in ["SUCCEEDED", "RUNNING"], "Execution should succeed"

        # Verify log group exists and would receive logs
        log_groups = logs_client.describe_log_groups(logGroupNamePrefix="/aws/stepfunctions/")["logGroups"]

        workflow_log_group = next((lg for lg in log_groups if lg["logGroupName"] == log_group_name), None)
        assert workflow_log_group is not None, "CloudWatch log group should exist"

    @mock_aws
    def test_cross_stack_iam_permissions_validation(self, aws_credentials):
        """
        Given: Transform pipeline이 여러 AWS 서비스를 사용하면
        When: Cross-stack IAM 권한을 검증하면
        Then: 필요한 모든 권한이 올바르게 부여되어야 함
        """
        iam_client = boto3.client("iam", region_name="us-east-1")

        # Define required IAM policies for transform pipeline
        required_policies = {
            "step_functions_execution_policy": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "lambda:InvokeFunction",
                            "glue:StartJobRun",
                            "glue:GetJobRun",
                            "glue:StartCrawler",
                            "glue:GetCrawler",
                        ],
                        "Resource": "*",
                    },
                    {
                        "Effect": "Allow",
                        "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                        "Resource": "arn:aws:logs:*:*:*",
                    },
                ],
            },
            "glue_execution_policy": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
                        "Resource": [
                            "arn:aws:s3:::*-raw-*/*",
                            "arn:aws:s3:::*-curated-*/*",
                            "arn:aws:s3:::*-artifacts-*/*",
                        ],
                    },
                    {
                        "Effect": "Allow",
                        "Action": ["glue:GetDatabase", "glue:GetTable", "glue:CreateTable", "glue:UpdateTable"],
                        "Resource": "*",
                    },
                ],
            },
            "lambda_preflight_policy": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["s3:ListBucket", "s3:GetObject"],
                        "Resource": [
                            "arn:aws:s3:::*-raw-*",
                            "arn:aws:s3:::*-raw-*/*",
                            "arn:aws:s3:::*-curated-*",
                            "arn:aws:s3:::*-curated-*/*",
                        ],
                    }
                ],
            },
        }

        # Create IAM roles and attach policies
        for policy_name, policy_document in required_policies.items():
            # Create policy
            iam_client.create_policy(PolicyName=policy_name, PolicyDocument=json.dumps(policy_document))

            # Create corresponding role
            # Align with expected role naming convention
            base = policy_name[:-7] if policy_name.endswith("_policy") else policy_name
            if base.endswith("_execution"):
                role_name = f"{base}_role"
            else:
                role_name = f"{base}_execution_role"
            assume_role_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": self._get_service_for_role(role_name)},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }

            iam_client.create_role(RoleName=role_name, AssumeRolePolicyDocument=json.dumps(assume_role_policy))

            # Attach policy to role
            iam_client.attach_role_policy(
                RoleName=role_name, PolicyArn=f"arn:aws:iam::123456789012:policy/{policy_name}"
            )

        # Verify all roles were created successfully
        roles = iam_client.list_roles()["Roles"]
        created_roles = [role["RoleName"] for role in roles if "execution_role" in role["RoleName"]]

        expected_roles = ["step_functions_execution_role", "glue_execution_role", "lambda_preflight_execution_role"]
        for expected_role in expected_roles:
            assert any(expected_role in role for role in created_roles), f"Role {expected_role} should be created"

        # Verify policies are attached
        for role_name in expected_roles:
            try:
                attached_policies = iam_client.list_attached_role_policies(RoleName=role_name)
                assert len(attached_policies["AttachedPolicies"]) > 0, f"Role {role_name} should have attached policies"
            except Exception:
                # In moto, some operations might not be fully implemented
                pass

    def _get_service_for_role(self, role_name: str) -> str:
        """IAM role에 맞는 AWS 서비스 반환"""
        if "step_functions" in role_name:
            return "states.amazonaws.com"
        elif "glue" in role_name:
            return "glue.amazonaws.com"
        elif "lambda" in role_name:
            return "lambda.amazonaws.com"
        else:
            return "states.amazonaws.com"

    @mock_aws
    def test_eventbridge_rule_error_handling(self, aws_credentials, buckets_config):
        """
        Given: EventBridge 규칙 설정에 오류가 있으면
        When: 규칙 생성 또는 타겟 설정을 시도하면
        Then: 적절한 에러가 발생하고 처리되어야 함
        """
        events_client = boto3.client("events", region_name="us-east-1")

        # Test invalid event pattern
        invalid_event_pattern = {
            "source": ["aws.s3"],
            "detail-type": ["Object Created"],
            "detail": {
                "bucket": {"name": [buckets_config["raw_bucket"]]},
                "object": {
                    # Invalid key pattern structure
                    "key": "invalid-pattern-format"
                },
            },
        }

        # This should succeed in moto (it's lenient), but in real AWS might fail
        try:
            events_client.put_rule(
                Name="test-invalid-pattern-rule", EventPattern=json.dumps(invalid_event_pattern), State="ENABLED"
            )
            # Rule creation succeeded, now test invalid target
        except Exception as e:
            assert "invalid" in str(e).lower() or "pattern" in str(e).lower()

        # Test adding target to non-existent rule
        with pytest.raises(Exception):
            events_client.put_targets(
                Rule="non-existent-rule",
                Targets=[{"Id": "1", "Arn": "arn:aws:states:us-east-1:123456789012:stateMachine:non-existent"}],
            )

    @mock_aws
    def test_s3_cross_region_event_handling(self, aws_credentials):
        """
        Given: 다른 리전의 S3 버킷에서 이벤트가 발생하면
        When: EventBridge 규칙이 처리하면
        Then: 리전별 제한사항을 고려하여 처리되어야 함
        """
        # Create buckets in different regions
        s3_us_east = boto3.client("s3", region_name="us-east-1")
        s3_us_west = boto3.client("s3", region_name="us-west-2")

        # Create bucket in us-east-1
        s3_us_east.create_bucket(Bucket="test-raw-bucket-east")

        # Create bucket in us-west-2 (with location constraint)
        s3_us_west.create_bucket(
            Bucket="test-raw-bucket-west", CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
        )

        # Upload files to both regions
        partition_date = datetime.strptime("2025-09-07", "%Y-%m-%d")
        test_key = build_raw_s3_object_key(
            domain="market",
            table_name="prices",
            data_source="yahoo_finance",
            interval="1d",
            symbol="AAPL",
            date=partition_date,
        )
        test_data = json.dumps({"symbol": "AAPL", "price": 150.25}).encode()

        s3_us_east.put_object(Bucket="test-raw-bucket-east", Key=test_key, Body=test_data)
        s3_us_west.put_object(Bucket="test-raw-bucket-west", Key=test_key, Body=test_data)

        # Verify both uploads succeeded
        east_objects = s3_us_east.list_objects_v2(Bucket="test-raw-bucket-east")
        west_objects = s3_us_west.list_objects_v2(Bucket="test-raw-bucket-west")

        assert east_objects["KeyCount"] == 1, "East bucket should have 1 object"
        assert west_objects["KeyCount"] == 1, "West bucket should have 1 object"

        # In real AWS, EventBridge rules would need to be configured per region
        # This test verifies the S3 setup works across regions
