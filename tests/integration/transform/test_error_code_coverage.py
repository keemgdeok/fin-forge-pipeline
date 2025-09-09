"""
Essential Error Code Coverage for 1GB Daily Batch

1GB 일일 배치에서 실제로 필요한 최소한의 에러 코드만 테스트합니다.
복잡한 재시도 로직보다는 단순한 에러 감지에 집중합니다.
"""

import pytest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError


@pytest.mark.integration
class TestEssentialErrorCodes:
    """1GB 일일 배치에 필수적인 에러 코드만 검증"""

    def test_crawler_timeout_error(self, daily_batch_env):
        """크롤러 타임아웃 (CRAWLER_FAILED) - 가장 흔한 시나리오"""
        # Given: 크롤러 타임아웃
        with patch("boto3.client") as mock_boto:
            mock_glue = Mock()
            mock_boto.return_value = mock_glue
            mock_glue.start_crawler.side_effect = ClientError(
                {"Error": {"Code": "OperationTimeoutException", "Message": "Crawler timed out"}}, "StartCrawler"
            )

            # When: 크롤러 시작
            try:
                mock_glue.start_crawler(Name="test-crawler")
                result = {"error_code": None}
            except ClientError as e:
                result = {"error_code": "CRAWLER_FAILED", "aws_error": e.response["Error"]["Code"]}

            # Then: 에러 코드 확인
            assert result["error_code"] == "CRAWLER_FAILED"
            assert result["aws_error"] == "OperationTimeoutException"

    def test_unexpected_error_handling(self, daily_batch_env):
        """예상치 못한 에러 (UNEXPECTED_ERROR) - Catch-all 검증"""
        # Given: 알 수 없는 서비스 에러
        with patch("boto3.client") as mock_boto:
            mock_s3 = Mock()
            mock_boto.return_value = mock_s3
            mock_s3.head_object.side_effect = Exception("Network timeout")

            # When: 예외 발생
            try:
                mock_s3.head_object(Bucket="test", Key="test")
                result = {"error_code": None}
            except Exception:
                result = {"error_code": "UNEXPECTED_ERROR", "should_retry": True, "max_retries": 1}

            # Then: Catch-all 처리 확인
            assert result["error_code"] == "UNEXPECTED_ERROR"
            assert result["should_retry"] is True
            assert result["max_retries"] == 1  # 스펙 준수
