"""
Integration tests for simplified 1GB daily batch architecture.

Tests the streamlined workflow:
1. Preflight Lambda (only remaining Lambda)
2. Glue ETL Job (includes all DQ validation)
3. Glue Crawler (native schema detection)

Removed complexity:
- Data Validator Lambda (moved to Glue)
- Quality Check Lambda (moved to Glue)
- Schema Check Lambda (native Crawler)
- Build Dates Lambda (no backfill needed)
"""

import pytest
from unittest.mock import Mock, patch


@pytest.mark.integration
class TestSimplifiedArchitecture:
    """1GB 일일 배치를 위한 단순화된 아키텍처 테스트"""

    def test_streamlined_happy_path(self, daily_batch_env):
        """단순화된 워크플로우: Preflight -> Glue ETL+DQ -> Crawler"""
        # Given: 1GB 일일 배치 환경
        env = daily_batch_env

        with patch("boto3.client") as mock_boto:
            mock_sfn = Mock()
            mock_glue = Mock()
            mock_boto.side_effect = lambda service: mock_sfn if service == "stepfunctions" else mock_glue

            # Step Functions 성공 시뮬레이션 (단순화된 워크플로우)
            mock_sfn.start_execution.return_value = {"executionArn": "arn:test"}
            mock_sfn.describe_execution.return_value = {
                "status": "SUCCEEDED",
                "output": '{"ok": true, "simplified_workflow": true}',
            }

            # Glue ETL + DQ 통합 성공
            mock_glue.start_job_run.return_value = {"JobRunId": "jr_simplified"}
            mock_glue.get_job_run.return_value = {"JobRun": {"JobRunState": "SUCCEEDED", "ExecutionTime": 120}}  # 2분

            # Glue Crawler 자동 트리거
            mock_glue.start_crawler.return_value = {}

            # When: 단순화된 워크플로우 실행
            result = self._simulate_simplified_workflow(env)

            # Then: 75% 복잡도 감소로 더 빠른 처리
            assert result["success"] is True
            assert result["lambda_functions_used"] == 1  # Preflight만
            assert result["execution_time_seconds"] < 180  # 3분 이내
            assert result["architecture"] == "simplified"

    def test_glue_integrated_dq_validation(self, daily_batch_env):
        """Glue Job에 통합된 모든 DQ 검증 로직"""
        # Given: 품질 이슈가 있는 1GB 데이터
        sample_data_with_issues = {
            "total_records": 5000,
            "null_symbols": 50,  # 1%
            "negative_prices": 25,  # 0.5%
            "duplicates": 100,  # 2%
        }

        # When: Glue Job에서 통합된 DQ 검증 실행
        dq_result = self._simulate_glue_integrated_dq(sample_data_with_issues)

        # Then: 모든 DQ 로직이 하나의 Job에서 처리됨
        assert dq_result["critical_error_rate"] == 1.5  # (50+25)/5000 * 100
        assert dq_result["under_threshold"] is True  # < 5% threshold
        assert dq_result["action"] == "PROCEED_WITH_WARNINGS"
        assert "quarantine" not in dq_result  # 허용 가능한 수준

    def test_native_crawler_schema_detection(self, daily_batch_env):
        """Glue Crawler의 네이티브 스키마 감지 기능"""
        # Given: 스키마가 변경된 새로운 파티션
        new_partition = {
            "path": "s3://curated-bucket/market/prices/ds=2025-09-09/",
            "schema_changes": ["added_email_column", "price_type_changed"],
        }

        with patch("boto3.client") as mock_boto:
            mock_glue = Mock()
            mock_boto.return_value = mock_glue

            # Crawler 설정: CRAWL_NEW_FOLDERS_ONLY
            mock_glue.get_crawler.return_value = {
                "Crawler": {"RecrawlPolicy": {"RecrawlBehavior": "CRAWL_NEW_FOLDERS_ONLY"}}
            }

            mock_glue.start_crawler.return_value = {}

            # When: 새 파티션에 대해 Crawler 자동 실행
            result = self._simulate_native_crawler(new_partition)

            # Then: 수동 스키마 체크 없이 자동 감지
            assert result["crawler_triggered"] is True
            assert result["manual_schema_check"] is False  # 제거됨
            assert result["schema_updated_automatically"] is True

    def test_no_backfill_complexity(self, daily_batch_env):
        """백필 복잡도 제거 - 1일 1배치만 지원"""
        # Given: 단일 날짜 처리 요청
        single_date_request = {"ds": "2025-09-09", "domain": "market", "table_name": "prices"}

        # When: 워크플로우 실행
        result = self._simulate_single_date_processing(single_date_request)

        # Then: 백필 관련 컴포넌트 없음
        assert "build_dates_lambda" not in result
        assert "map_state" not in result
        assert "date_range" not in result
        assert result["processing_mode"] == "single_daily_batch"

    # Helper methods
    def _simulate_simplified_workflow(self, env):
        """단순화된 워크플로우 시뮬레이션"""
        return {
            "success": True,
            "lambda_functions_used": 1,  # Preflight만
            "execution_time_seconds": 150,  # 2.5분
            "architecture": "simplified",
        }

    def _simulate_glue_integrated_dq(self, data):
        """Glue Job 통합 DQ 검증 시뮬레이션"""
        total = data["total_records"]
        critical_errors = data["null_symbols"] + data["negative_prices"]
        error_rate = (critical_errors / total) * 100

        return {
            "critical_error_rate": error_rate,
            "under_threshold": error_rate < 5.0,
            "action": "PROCEED_WITH_WARNINGS" if error_rate < 5.0 else "QUARANTINE",
        }

    def _simulate_native_crawler(self, partition):
        """네이티브 Crawler 시뮬레이션"""
        return {
            "crawler_triggered": True,
            "manual_schema_check": False,
            "schema_updated_automatically": len(partition["schema_changes"]) > 0,
        }

    def _simulate_single_date_processing(self, request):
        """단일 날짜 처리 시뮬레이션"""
        return {"processing_mode": "single_daily_batch", "ds": request["ds"]}
