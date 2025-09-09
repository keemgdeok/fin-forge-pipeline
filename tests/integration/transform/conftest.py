"""
Simplified Test Configuration for 1GB Daily Batch

1GB 일일 배치에 특화된 간소화된 테스트 설정입니다.
복잡도를 최소화하면서 핵심 기능만 검증합니다.
"""

import pytest
import os
from typing import Dict
from unittest.mock import Mock, patch


@pytest.fixture
def daily_batch_env() -> Dict[str, str]:
    """1GB 일일 배치 처리를 위한 핵심 환경 변수"""
    return {
        "ENVIRONMENT": "test",
        "AWS_REGION": "ap-northeast-2",
        "TARGET_DATE": "2025-09-09",
        "DOMAIN": "customer-data",
        "TABLE": "customers",
        "RAW_BUCKET": "finge-raw-test",
        "CURATED_BUCKET": "finge-curated-test",
        "MAX_DPU": "2",  # 1GB에 충분한 최소 DPU
    }


@pytest.fixture
def mock_aws_services():
    """핵심 AWS 서비스 Mock (S3, Glue, Step Functions)"""
    with patch("boto3.client") as mock_boto:
        # 간단한 성공 시나리오 Mock
        mock_s3 = Mock()
        mock_s3.head_object.return_value = {"ContentLength": 1024 * 1024}  # 1MB

        mock_glue = Mock()
        mock_glue.start_job_run.return_value = {"JobRunId": "jr_123"}
        mock_glue.get_job_run.return_value = {"JobRun": {"JobRunState": "SUCCEEDED", "ExecutionTime": 180}}

        mock_sfn = Mock()
        mock_sfn.describe_execution.return_value = {"status": "SUCCEEDED", "output": '{"ok": true, "rowCount": 5000}'}

        def get_client(service_name, **kwargs):
            if service_name == "s3":
                return mock_s3
            elif service_name == "glue":
                return mock_glue
            elif service_name == "stepfunctions":
                return mock_sfn
            return Mock()

        mock_boto.side_effect = get_client
        yield {"s3": mock_s3, "glue": mock_glue, "stepfunctions": mock_sfn}


@pytest.fixture
def sample_batch_data():
    """1GB 배치에 적합한 현실적인 샘플 데이터"""
    return {
        "record_count": 5000,  # 1GB ≈ 5K records
        "processing_time_limit": 300,  # 5분 제한
        "quality_threshold": 0.95,  # 95% 품질
        "file_size_mb": 1024,  # 1GB
        "expected_partitions": 1,  # 단일 일일 파티션
    }


# 간소화된 헬퍼 함수들
def step_function_test_env():
    """Step Functions 테스트를 위한 최소 환경"""
    os.environ.update(
        {
            "STATE_MACHINE_ARN": "arn:aws:states:ap-northeast-2:123456789012:stateMachine:test",
            "EXECUTION_ROLE_ARN": "arn:aws:iam::123456789012:role/test-role",
        }
    )


def generate_test_records(count: int = 5000):
    """간단한 테스트 레코드 생성"""
    return [
        {
            "id": i + 1,
            "name": f"Customer_{i+1}",
            "email": f"user{i+1}@example.com",
            "created_at": "2025-09-09T10:00:00Z",
        }
        for i in range(count)
    ]
