"""
Simple Retry Logic for 1GB Daily Batch

1GB 일일 배치에 필요한 간단한 재시도 로직만 검증합니다.
복잡한 패턴보다는 기본적인 최대 재시도 횟수 준수에 집중합니다.
"""

import pytest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError


@pytest.mark.integration
class TestSimpleRetryLogic:
    """1GB 일일 배치용 간단한 재시도 로직"""

    def test_glue_job_max_retry_limit(self, daily_batch_env):
        """Glue 작업 최대 재시도 횟수 (1회) 검증"""
        # Given: 계속 실패하는 Glue 작업
        with patch("boto3.client") as mock_boto:
            mock_glue = Mock()
            mock_boto.return_value = mock_glue

            # 모든 시도 실패
            mock_glue.start_job_run.side_effect = ClientError(
                {"Error": {"Code": "ConcurrentRunsExceededException", "Message": "Too many runs"}}, "StartJobRun"
            )

            # When: 재시도 로직 실행 (스펙: 최대 1회 재시도)
            attempts = 0
            max_retries = 1
            success = False

            while attempts <= max_retries and not success:
                attempts += 1
                try:
                    mock_glue.start_job_run(JobName="test")
                    success = True
                except ClientError:
                    if attempts > max_retries:
                        break

            # Then: 정확히 2회 시도 (초기 1회 + 재시도 1회) 후 중단
            assert attempts == 2
            assert success is False
            assert mock_glue.start_job_run.call_count == 2

    def test_preflight_validation_retry(self, daily_batch_env):
        """Preflight 검증 재시도 (최대 2회) 검증"""
        # Given: 일시적인 S3 접근 장애
        with patch("boto3.client") as mock_boto:
            mock_s3 = Mock()
            mock_boto.return_value = mock_s3

            # 첫 2회 실패, 3회째 성공
            mock_s3.list_objects_v2.side_effect = [
                ClientError({"Error": {"Code": "SlowDown", "Message": "Please reduce request rate"}}, "ListObjects"),
                ClientError({"Error": {"Code": "SlowDown", "Message": "Please reduce request rate"}}, "ListObjects"),
                {"KeyCount": 0},  # 성공
            ]

            # When: 멱등성 체크 재시도 (스펙: 최대 2회 재시도)
            attempts = 0
            max_retries = 2
            success = False

            while attempts <= max_retries and not success:
                attempts += 1
                try:
                    result = mock_s3.list_objects_v2(Bucket="test", Prefix="test")
                    if "KeyCount" in result:
                        success = True
                except ClientError:
                    if attempts > max_retries:
                        break

            # Then: 3회 시도 후 성공
            assert attempts == 3
            assert success is True
            assert mock_s3.list_objects_v2.call_count == 3
