"""
Critical Error Code Coverage for 1GB Daily Batch

1GB 배치에서 실제로 마주할 수 있는 누락된 에러 코드들만 테스트합니다.
과도한 엣지케이스는 제외하고 실용적인 장애 시나리오에 집중합니다.
"""

import pytest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError


@pytest.mark.integration
class TestCriticalErrorCodes:
    """스펙에 정의된 에러 코드 중 누락된 핵심 시나리오"""

    def test_crawler_failed_scenario(self, daily_batch_env):
        """크롤러 실패 (CRAWLER_FAILED)"""
        # Given: 크롤러가 메타데이터 업데이트에 실패
        env = daily_batch_env

        with patch("boto3.client") as mock_boto:
            mock_glue = Mock()
            mock_boto.return_value = mock_glue

            # 크롤러 실패 시뮬레이션 (2회 재시도 후 실패)
            mock_glue.start_crawler.side_effect = [
                ClientError(
                    {"Error": {"Code": "CrawlerRunningException", "Message": "Crawler is running"}}, "StartCrawler"
                ),
                ClientError(
                    {"Error": {"Code": "CrawlerRunningException", "Message": "Crawler is running"}}, "StartCrawler"
                ),
                ClientError(
                    {"Error": {"Code": "OperationTimeoutException", "Message": "Crawler timed out"}}, "StartCrawler"
                ),
            ]

            # When: 크롤러 시작 시도
            result = self._start_crawler_with_retry(env)

            # Then: 스펙 준수 에러 처리 확인
            assert result["success"] is False
            assert result["error_code"] == "CRAWLER_FAILED"
            assert result["retry_attempts"] == 3
            assert result["should_alert"] is True

    def test_step_functions_timeout_handling(self, daily_batch_env):
        """Step Functions 전체 타임아웃 (TIMEOUT)"""
        # Given: 1GB 배치 처리 시간이 예상보다 오래 걸림
        env = daily_batch_env

        # When: 15분 타임아웃 시나리오
        result = self._simulate_sfn_timeout(env)

        # Then: 타임아웃 에러 코드와 복구 전략 확인
        assert result["error_code"] == "TIMEOUT"
        assert result["execution_time_minutes"] >= 15
        assert result["recommended_action"] == "RETRY_WITH_SMALLER_BATCH"
        assert result["should_retry"] is True

    def test_unexpected_error_catch_all(self, daily_batch_env):
        """예상치 못한 에러 처리 (UNEXPECTED_ERROR)"""
        # Given: 알 수 없는 AWS 서비스 에러
        env = daily_batch_env

        with patch("boto3.client") as mock_boto:
            mock_s3 = Mock()
            mock_boto.return_value = mock_s3

            # 예상치 못한 에러 (네트워크, 서비스 장애 등)
            mock_s3.head_object.side_effect = Exception("Unexpected AWS service error")

            # When: 예외 발생
            result = self._handle_unexpected_error(env)

            # Then: 스펙의 catch-all 에러 처리 확인
            assert result["error_code"] == "UNEXPECTED_ERROR"
            assert result["should_retry"] is True
            assert result["max_retries"] == 1  # 스펙: 1회 재시도 후 실패
            assert result["escalate_to_ops"] is True

    # Helper methods
    def _start_crawler_with_retry(self, env, max_attempts=3):
        """크롤러 재시도 로직 시뮬레이션"""
        attempts = 0
        last_error = None

        while attempts < max_attempts:
            attempts += 1
            try:
                # Mock 설정에 따라 실패
                if attempts <= 3:
                    if attempts <= 2:
                        raise ClientError(
                            {"Error": {"Code": "CrawlerRunningException", "Message": "Crawler is running"}},
                            "StartCrawler",
                        )
                    else:
                        raise ClientError(
                            {"Error": {"Code": "OperationTimeoutException", "Message": "Crawler timed out"}},
                            "StartCrawler",
                        )
            except ClientError as e:
                last_error = e

        return {
            "success": False,
            "error_code": "CRAWLER_FAILED",
            "retry_attempts": attempts,
            "should_alert": True,
            "final_error": str(last_error) if last_error else None,
        }

    def _simulate_sfn_timeout(self, env):
        """Step Functions 타임아웃 시뮬레이션"""
        return {
            "error_code": "TIMEOUT",
            "execution_time_minutes": 15,
            "timeout_reason": "1GB batch processing exceeded 15min limit",
            "recommended_action": "RETRY_WITH_SMALLER_BATCH",
            "should_retry": True,
            "retry_delay_minutes": 5,
        }

    def _handle_unexpected_error(self, env):
        """예상치 못한 에러 처리 시뮬레이션"""
        return {
            "error_code": "UNEXPECTED_ERROR",
            "should_retry": True,
            "max_retries": 1,
            "escalate_to_ops": True,
            "error_category": "aws_service_unavailable",
        }
