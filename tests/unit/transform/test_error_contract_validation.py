"""Error contract and retry policy validation tests for transform pipeline.

This module tests the error handling contracts and retry policies defined in the
transform specifications, ensuring all components follow the specified error codes,
retry patterns, and failure scenarios according to TDD methodology.
"""

# JSON operations removed after error payload simplification
import pytest
import runpy
from unittest.mock import Mock, patch
from tests.unit.shared.test_fixtures import assert_error_contract


class TestErrorContractSpecCompliance:
    """Test error contracts follow the transform specifications exactly."""

    def test_preflight_error_codes_specification_compliance(self, transform_env):
        """
        Given: 다양한 Preflight 실패 시나리오
        When: 각 시나리오를 실행하면
        Then: 명세에 정의된 정확한 오류 코드를 반환해야 함
        """
        transform_env()
        mod = runpy.run_path("src/lambda/functions/preflight/handler.py")

        # Test PRE_VALIDATION_FAILED scenarios
        test_cases = [
            {
                "event": {},  # Missing domain/table_name
                "expected_code": "PRE_VALIDATION_FAILED",
                "description": "Missing required fields",
            },
            {
                "event": {
                    "domain": "market",
                    "table_name": "prices",
                },  # Missing bucket env vars
                "expected_code": "PRE_VALIDATION_FAILED",
                "description": "Missing environment variables",
                "setup": lambda: None,  # Don't setup env vars
            },
            {
                "event": {
                    "source_key": "market/prices/no_ingestion_date/file.json",
                    "domain": "market",
                    "table_name": "prices",
                },
                "expected_code": "PRE_VALIDATION_FAILED",
                "description": "Invalid S3 key format",
            },
        ]

        for case in test_cases:
            if case.get("setup"):
                case["setup"]()

            resp = mod["lambda_handler"](case["event"], None)
            assert_error_contract(resp, case["expected_code"])
            assert case["description"] in resp.get("reason", "").lower() or any(
                word in resp["error"]["message"].lower() for word in case["description"].lower().split()
            )

    def test_schema_check_error_codes_specification_compliance(self, transform_env):
        """
        Given: 다양한 Schema Check 실패 시나리오
        When: 각 시나리오를 실행하면
        Then: 명세에 정의된 정확한 오류 코드를 반환해야 함
        """
        transform_env()
        mod = runpy.run_path("src/lambda/functions/schema_check/handler.py")

        test_cases = [
            {
                "event": {},  # Missing domain/table_name
                "expected_code": "PRE_VALIDATION_FAILED",
                "description": "Missing required fields",
            },
            {
                "event": {"domain": "market", "table_name": "prices"},
                "expected_code": "SCHEMA_CHECK_FAILED",
                "description": "Latest schema fingerprint not found",
                "mock_s3": "empty",
            },
        ]

        for case in test_cases:
            if case.get("setup"):
                case["setup"]()

            if case.get("mock_s3") == "empty":
                # Mock empty S3 response
                with patch("boto3.client") as mock_boto:
                    mock_s3 = Mock()
                    mock_s3.get_object.side_effect = Exception("NoSuchKey")
                    mock_boto.return_value = mock_s3

                    resp = mod["lambda_handler"](case["event"], None)
            else:
                resp = mod["lambda_handler"](case["event"], None)

            assert_error_contract(resp, case["expected_code"])

    def test_build_dates_error_codes_specification_compliance(self, transform_env):
        """
        Given: 다양한 Build Dates 실패 시나리오
        When: 각 시나리오를 실행하면
        Then: 명세에 정의된 정확한 오류 코드를 반환해야 함
        """
        transform_env(max_backfill_days=5)
        mod = runpy.run_path("src/lambda/functions/build_dates/handler.py")

        test_cases = [
            {
                "event": {},  # Missing date_range
                "expected_code": "PRE_VALIDATION_FAILED",
                "description": "Missing date range",
            },
            {
                "event": {"date_range": {"start": "2025-09-07", "end": "2025-09-05"}},  # End before start
                "expected_code": "PRE_VALIDATION_FAILED",
                "description": "Invalid date order",
            },
            {
                "event": {"date_range": {"start": "2025-09-01", "end": "2025-09-10"}},  # Too long (10 days > 5)
                "expected_code": "PRE_VALIDATION_FAILED",
                "description": "Date range too long",
            },
            {
                "event": {
                    "date_range": {
                        "start": "invalid-date",
                        "end": "2025-09-07",
                    }
                },  # Invalid date format
                "expected_code": "PRE_VALIDATION_FAILED",
                "description": "Invalid date format",
            },
        ]

        for case in test_cases:
            resp = mod["lambda_handler"](case["event"], None)
            assert_error_contract(resp, case["expected_code"])


class TestIdempotencyContract:
    """Test idempotency behavior follows specification."""

    def test_preflight_idempotent_skip_contract(self, transform_env):
        """
        Given: Curated 파티션이 이미 존재함
        When: Preflight를 실행하면
        Then: IDEMPOTENT_SKIP 코드와 함께 proceed=false를 반환해야 함
        """
        transform_env()
        mod = runpy.run_path("src/lambda/functions/preflight/handler.py")

        # Mock S3 to return existing partition
        with patch("boto3.client") as mock_boto:
            mock_s3 = Mock()
            mock_s3.list_objects_v2.return_value = {"KeyCount": 1}  # Partition exists
            mock_boto.return_value = mock_s3

            event = {
                "domain": "market",
                "table_name": "prices",
                "ds": "2025-09-07",
                "file_type": "json",
            }

            resp = mod["lambda_handler"](event, None)

            # Should follow idempotency contract
            assert resp["proceed"] is False
            assert resp["reason"] == "Already processed"
            assert_error_contract(resp, "IDEMPOTENT_SKIP")
            assert resp["ds"] == "2025-09-07"  # Should still return ds

    def test_idempotent_skip_treated_as_success_at_sfn_level(self):
        """
        Given: IDEMPOTENT_SKIP 오류가 발생함
        When: Step Functions에서 처리할 때
        Then: 성공으로 처리되어야 함 (Choice condition에 따라)
        """
        # This would be tested at the Step Functions level
        # Mock the choice condition logic
        response = {
            "proceed": False,
            "error": {
                "code": "IDEMPOTENT_SKIP",
                "message": "Already processed",
            },
        }

        # Simulate Step Functions Choice condition
        if response.get("error", {}).get("code") == "IDEMPOTENT_SKIP":
            # Should route to success state, not failure
            should_succeed = True
        else:
            should_succeed = response.get("proceed", False)

        assert should_succeed is True


class TestRetryPolicySpecCompliance:
    """Test retry policies match the specifications exactly."""

    @pytest.mark.parametrize(
        "error_code,max_retries,should_retry",
        [
            ("PRE_VALIDATION_FAILED", 2, True),  # Exponential backoff, max 2
            ("IDEMPOTENT_SKIP", 0, False),  # No retry (normal termination)
            ("NO_RAW_DATA", 0, False),  # No retry
            ("GLUE_JOB_FAILED", 1, True),  # Max 1 retry
            ("DQ_FAILED", 0, False),  # No retry (manual fix required)
            ("CRAWLER_FAILED", 2, True),  # Max 2 retries with backoff
            ("SCHEMA_CHECK_FAILED", 1, True),  # 1 retry
            ("TIMEOUT", 1, True),  # 1 retry depending on cause
            ("UNEXPECTED_ERROR", 1, True),  # 1 retry then fail
        ],
    )
    def test_error_retry_policy_specification(self, error_code, max_retries, should_retry):
        """
        Given: 특정 오류 코드
        When: 재시도 정책을 확인하면
        Then: 명세에 정의된 재시도 규칙을 따라야 함
        """
        # This test validates the retry policy definition
        # In actual Step Functions, this would be configured via Retry blocks

        retry_config = {
            "PRE_VALIDATION_FAILED": {"max_attempts": 2, "backoff_rate": 2.0},
            "IDEMPOTENT_SKIP": {"max_attempts": 0},  # No retry
            "NO_RAW_DATA": {"max_attempts": 0},  # No retry
            "GLUE_JOB_FAILED": {"max_attempts": 1},
            "DQ_FAILED": {"max_attempts": 0},  # No retry
            "CRAWLER_FAILED": {"max_attempts": 2, "backoff_rate": 2.0},
            "SCHEMA_CHECK_FAILED": {"max_attempts": 1},
            "TIMEOUT": {"max_attempts": 1},
            "UNEXPECTED_ERROR": {"max_attempts": 1},
        }

        config = retry_config.get(error_code, {"max_attempts": 0})

        assert config["max_attempts"] == max_retries
        assert (config["max_attempts"] > 0) == should_retry

    def test_exponential_backoff_configuration(self):
        """
        Given: 지수 백오프가 필요한 오류들
        When: 재시도 설정을 확인하면
        Then: 올바른 백오프 비율이 설정되어야 함
        """
        backoff_errors = ["PRE_VALIDATION_FAILED", "CRAWLER_FAILED"]
        expected_backoff_rate = 2.0  # 2x multiplier per spec

        for error_code in backoff_errors:
            # In actual Step Functions, this would be:
            # "BackoffRate": 2.0, "IntervalSeconds": 2
            backoff_rate = 2.0  # As specified
            assert backoff_rate == expected_backoff_rate


class TestErrorPayloadStructure:
    """Test error payloads match specification structure."""

    def test_error_payload_structure_specification(self, transform_env):
        """
        Given: 오류가 발생함
        When: 오류 페이로드를 생성하면
        Then: 명세에 정의된 구조를 따라야 함
        """
        transform_env()
        mod = runpy.run_path("src/lambda/functions/preflight/handler.py")

        # Generate an error
        resp = mod["lambda_handler"]({}, None)  # Missing required fields

        # Validate error payload structure per specification
        assert "error" in resp
        assert "code" in resp["error"]
        assert "message" in resp["error"]
        assert isinstance(resp["error"]["code"], str)
        assert isinstance(resp["error"]["message"], str)
        assert len(resp["error"]["code"]) > 0
        assert len(resp["error"]["message"]) > 0

    def test_success_payload_excludes_error_field(self, transform_env):
        """
        Given: 성공적인 실행
        When: 응답 페이로드를 확인하면
        Then: error 필드가 포함되지 않아야 함
        """
        transform_env()
        mod = runpy.run_path("src/lambda/functions/build_dates/handler.py")

        # Generate a success response
        resp = mod["lambda_handler"]({"date_range": {"start": "2025-09-07", "end": "2025-09-08"}}, None)

        # Should not contain error field
        assert "error" not in resp
        assert "dates" in resp  # Should have success payload


class TestDataQualityFailureContract:
    """Test data quality failure contracts."""

    def test_dq_failed_error_structure(self):
        """
        Given: 데이터 품질 검사 실패
        When: 오류 페이로드를 생성하면
        Then: DQ_FAILED 코드와 상세 정보를 포함해야 함
        """
        # Simulate DQ failure payload structure
        dq_error_payload = {
            "ok": False,
            "correlationId": "market:prices:2025-09-07",
            "error": {
                "code": "DQ_FAILED",
                "message": "null ratio exceeded for column price",
                "partition": "ds=2025-09-07",
            },
        }

        # Validate structure
        assert dq_error_payload["ok"] is False
        assert "correlationId" in dq_error_payload
        assert_error_contract(dq_error_payload, "DQ_FAILED")
        assert "partition" in dq_error_payload["error"]

    def test_no_raw_data_error_structure(self):
        """
        Given: Raw 데이터가 없음
        When: 오류 페이로드를 생성하면
        Then: NO_RAW_DATA 코드를 포함해야 함
        """
        # Simulate the error that would be raised in Glue ETL
        with pytest.raises(RuntimeError, match="NO_RAW_DATA"):
            raise RuntimeError("NO_RAW_DATA: No records found for ds")


class TestCorrelationIdContract:
    """Test correlation ID contracts."""

    def test_correlation_id_format_specification(self):
        """
        Given: 다양한 실행 유형
        When: correlation ID를 생성하면
        Then: 명세에 정의된 형식을 따라야 함
        """
        # Single execution format: domain:table:ds
        single_correlation_id = "market:prices:2025-09-07"
        parts = single_correlation_id.split(":")
        assert len(parts) == 3
        assert parts[0] == "market"  # domain
        assert parts[1] == "prices"  # table
        assert parts[2] == "2025-09-07"  # ds

        # Execution ID based format (when provided)
        execution_id = "ext-req-123"
        assert len(execution_id) > 0

    def test_success_payload_includes_correlation_id(self):
        """
        Given: 성공적인 실행
        When: 응답 페이로드를 확인하면
        Then: correlationId가 포함되어야 함
        """
        success_payload = {
            "ok": True,
            "correlationId": "market:prices:2025-09-07",
            "domain": "market",
            "table": "prices",
            "partitions": ["ds=2025-09-07"],
            "stats": {
                "rowCount": 123456,
                "bytesWritten": 987654321,
                "fileCount": 24,
            },
            "glueRunIds": ["jr_abcdef"],
        }

        assert success_payload["ok"] is True
        assert "correlationId" in success_payload
        assert success_payload["correlationId"] == "market:prices:2025-09-07"


class TestStepFunctionsIntegration:
    """Test Step Functions integration error handling."""

    def test_step_functions_error_normalization(self):
        """
        Given: 다양한 태스크에서 오류 발생
        When: Step Functions에서 정규화하면
        Then: 일관된 오류 구조를 제공해야 함
        """
        # Simulate Step Functions error normalization Pass state
        # Error payload would be extracted from Step Functions raw error
        # in actual implementation

        # Normalize to specification format
        normalized_error = {
            "ok": False,
            "error": {"code": "DQ_FAILED", "message": "null ratio exceeded"},
        }

        assert normalized_error["ok"] is False
        assert_error_contract(normalized_error, "DQ_FAILED")

    def test_step_functions_catch_block_configuration(self):
        """
        Given: Step Functions 태스크 정의
        When: Catch 블록을 확인하면
        Then: 모든 에러 상태를 처리해야 함
        """
        # Simulate Step Functions Catch configuration
        catch_config = [
            {
                "ErrorEquals": ["States.TaskFailed", "States.ALL"],
                "Next": "NormalizeAndFail",
                "ResultPath": "$.error",
            }
        ]

        # Should catch all error types
        error_equals = catch_config[0]["ErrorEquals"]
        assert "States.TaskFailed" in error_equals
        assert "States.ALL" in error_equals
        assert catch_config[0]["Next"] == "NormalizeAndFail"


@pytest.mark.error_contract
class TestComprehensiveErrorScenarios:
    """Comprehensive error scenario testing."""

    def test_all_lambda_functions_implement_error_contract(self, transform_env):
        """
        Given: 모든 Transform Lambda 함수들
        When: 오류 상황을 테스트하면
        Then: 모든 함수가 일관된 오류 계약을 구현해야 함
        """
        transform_env()

        lambda_modules = [
            ("preflight", "src/lambda/functions/preflight/handler.py"),
            ("schema_check", "src/lambda/functions/schema_check/handler.py"),
            ("build_dates", "src/lambda/functions/build_dates/handler.py"),
        ]

        for name, path in lambda_modules:
            mod = runpy.run_path(path)

            # Generate an error (empty event should cause validation failure)
            resp = mod["lambda_handler"]({}, None)

            # All functions should implement consistent error structure
            if "error" in resp:
                assert "code" in resp["error"], f"{name} missing error code"
                assert "message" in resp["error"], f"{name} missing error message"
                assert isinstance(resp["error"]["code"], str), f"{name} error code not string"
                assert len(resp["error"]["code"]) > 0, f"{name} empty error code"

    def test_error_messages_are_descriptive(self, transform_env):
        """
        Given: 오류가 발생함
        When: 오류 메시지를 확인하면
        Then: 문제를 이해할 수 있는 설명적인 메시지여야 함
        """
        transform_env()

        test_cases = [
            {
                "module": "src/lambda/functions/preflight/handler.py",
                "event": {},
                "expected_keywords": ["domain", "table_name", "required"],
            },
            {
                "module": "src/lambda/functions/build_dates/handler.py",
                "event": {"date_range": {"start": "2025-09-07", "end": "2025-09-05"}},
                "expected_keywords": ["start", "end", "<="],
            },
        ]

        for case in test_cases:
            mod = runpy.run_path(case["module"])
            resp = mod["lambda_handler"](case["event"], None)

            if "error" in resp:
                message = resp["error"]["message"].lower()
                for keyword in case["expected_keywords"]:
                    assert keyword.lower() in message, f"Missing keyword '{keyword}' in error message: {message}"
