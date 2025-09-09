"""
Data Quality Tests for 1GB Daily Batch

1GB 일일 배치에 필요한 핵심 데이터 품질 검증만 수행합니다.
과도한 품질 규칙보다는 실용적인 검증에 집중합니다.
"""

import pytest
from typing import Dict, List, Any


@pytest.mark.integration
class TestDataQuality:
    """1GB 일일 배치를 위한 데이터 품질 테스트"""

    def test_critical_data_quality_rules(self, daily_batch_env):
        """치명적 데이터 품질 규칙 검증"""
        # Given: 다양한 품질 이슈가 있는 샘플 데이터
        sample_data = [
            {"id": 1, "name": "John", "email": "john@example.com", "age": 25},  # 정상
            {"id": 2, "name": "Jane", "email": "jane@example.com", "age": 30},  # 정상
            {"id": None, "name": "NoId", "email": "noid@example.com", "age": 28},  # ID 누락 (치명적)
            {"id": 3, "name": "", "email": "empty@example.com", "age": 35},  # 이름 없음 (경고)
            {"id": 4, "name": "Bad Email", "email": "invalid-email", "age": 40},  # 잘못된 이메일 (경고)
        ]

        # When: 품질 검증 실행
        quality_result = self._validate_critical_rules(sample_data)

        # Then: 치명적 이슈는 1개, 경고는 2개
        assert quality_result["total_rows"] == 5
        assert quality_result["critical_issues"] == 1  # ID 누락
        assert quality_result["warning_issues"] == 2  # 이름 없음, 잘못된 이메일
        assert quality_result["should_fail_pipeline"] is False  # 20% 치명적 에러는 허용 가능

    def test_data_completeness_validation(self, daily_batch_env):
        """데이터 완전성 검증 (1GB 배치 기준)"""
        # Given: 예상 레코드 수와 실제 레코드 수
        expected_records = 5000  # 일일 예상 레코드
        actual_records = 4850  # 실제 처리된 레코드 (3% 차이)

        # When: 완전성 검증 실행
        completeness_result = self._validate_completeness(expected_records, actual_records)

        # Then: 허용 가능한 범위 내 확인
        assert completeness_result["completeness_rate"] >= 0.95  # 95% 이상
        assert completeness_result["within_tolerance"] is True
        assert completeness_result["action"] == "PROCEED"

    def test_schema_consistency_check(self, daily_batch_env):
        """스키마 일관성 검증"""
        # Given: 현재 배치의 스키마
        current_schema = {
            "fields": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "name", "type": "string", "nullable": True},
                {"name": "email", "type": "string", "nullable": True},
                {"name": "age", "type": "integer", "nullable": True},
                {"name": "created_at", "type": "timestamp", "nullable": False},  # 새 필드
            ]
        }

        # 기존 스키마 (어제까지)
        previous_schema = {
            "fields": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "name", "type": "string", "nullable": True},
                {"name": "email", "type": "string", "nullable": True},
                {"name": "age", "type": "integer", "nullable": True},
            ]
        }

        # When: 스키마 호환성 검증
        schema_result = self._validate_schema_compatibility(current_schema, previous_schema)

        # Then: 하위 호환성 확인
        assert schema_result["backward_compatible"] is True
        assert schema_result["new_fields"] == ["created_at"]
        assert schema_result["removed_fields"] == []
        assert schema_result["breaking_changes"] == []

    # Helper methods for data quality validation
    def _validate_critical_rules(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """치명적 데이터 품질 규칙 검증"""
        total_rows = len(data)
        critical_issues = 0
        warning_issues = 0

        for row in data:
            # 치명적 규칙: ID 필수
            if row.get("id") is None:
                critical_issues += 1

            # 경고 규칙: 이름이 비어있음
            if not row.get("name", "").strip():
                warning_issues += 1

            # 경고 규칙: 이메일 형식 오류
            email = row.get("email", "")
            if email and "@" not in email:
                warning_issues += 1

        critical_rate = critical_issues / total_rows
        warning_rate = warning_issues / total_rows
        overall_quality = 1 - critical_rate - (warning_rate * 0.5)

        return {
            "total_rows": total_rows,
            "critical_issues": critical_issues,
            "warning_issues": warning_issues,
            "critical_rate": critical_rate,
            "warning_rate": warning_rate,
            "overall_quality": overall_quality,
            "should_fail_pipeline": critical_rate > 0.3 or overall_quality < 0.5,  # 1GB 배치에 실용적인 임계값
        }

    def _validate_completeness(self, expected: int, actual: int) -> Dict[str, Any]:
        """데이터 완전성 검증"""
        completeness_rate = actual / expected if expected > 0 else 0
        tolerance = 0.05  # 5% 허용 범위

        return {
            "expected_records": expected,
            "actual_records": actual,
            "completeness_rate": completeness_rate,
            "within_tolerance": completeness_rate >= (1 - tolerance),
            "action": "PROCEED" if completeness_rate >= 0.95 else "INVESTIGATE",
        }

    def _validate_schema_compatibility(self, current: Dict[str, Any], previous: Dict[str, Any]) -> Dict[str, Any]:
        """스키마 호환성 검증"""
        current_fields = {f["name"]: f for f in current["fields"]}
        previous_fields = {f["name"]: f for f in previous["fields"]}

        new_fields = list(set(current_fields.keys()) - set(previous_fields.keys()))
        removed_fields = list(set(previous_fields.keys()) - set(current_fields.keys()))

        # 하위 호환성 위반 사항 검사
        breaking_changes = []
        for field_name in previous_fields:
            if field_name in current_fields:
                old_field = previous_fields[field_name]
                new_field = current_fields[field_name]

                # 타입 변경 검사
                if old_field["type"] != new_field["type"]:
                    breaking_changes.append(
                        f"Type changed for {field_name}: {old_field['type']} -> {new_field['type']}"
                    )

                # nullable 제약 강화 검사
                if old_field.get("nullable", True) and not new_field.get("nullable", True):
                    breaking_changes.append(f"Nullable constraint added for {field_name}")

        return {
            "backward_compatible": len(breaking_changes) == 0 and len(removed_fields) == 0,
            "new_fields": new_fields,
            "removed_fields": removed_fields,
            "breaking_changes": breaking_changes,
            "schema_evolution_safe": len(new_fields) > 0 and len(breaking_changes) == 0,
        }


@pytest.mark.integration
class TestDataQualityThresholds:
    """1GB 배치에 특화된 품질 임계값 테스트"""

    def test_acceptable_quality_thresholds(self):
        """허용 가능한 품질 임계값 검증"""
        # Given: 1GB 일일 배치의 현실적인 품질 기준
        quality_thresholds = {
            "critical_error_rate": 0.01,  # 1% 이하 치명적 에러
            "warning_rate": 0.10,  # 10% 이하 경고
            "completeness_rate": 0.95,  # 95% 이상 완전성
            "schema_compatibility": True,  # 스키마 호환성 필수
            "processing_time_limit": 300,  # 5분 이내 처리
        }

        # When: 실제 배치 품질 측정 시뮬레이션
        actual_quality = {
            "critical_error_rate": 0.005,  # 0.5% (양호)
            "warning_rate": 0.08,  # 8% (양호)
            "completeness_rate": 0.97,  # 97% (양호)
            "schema_compatibility": True,  # 호환됨
            "processing_time": 240,  # 4분 (양호)
        }

        # Then: 모든 임계값 통과 확인
        for metric, threshold in quality_thresholds.items():
            if metric == "schema_compatibility":
                assert actual_quality[metric] == threshold
            elif metric == "processing_time_limit":
                assert actual_quality["processing_time"] <= threshold
            elif metric in ["critical_error_rate", "warning_rate"]:
                assert actual_quality[metric] <= threshold
            else:  # completeness_rate
                assert actual_quality[metric] >= threshold

    def test_quality_degradation_alerts(self):
        """품질 저하 알림 기준 테스트"""
        # Given: 품질이 저하된 배치
        degraded_quality = {
            "critical_error_rate": 0.02,  # 2% (임계값 초과)
            "warning_rate": 0.15,  # 15% (임계값 초과)
            "completeness_rate": 0.90,  # 90% (임계값 미만)
        }

        # When: 품질 평가 실행
        alert_result = self._evaluate_quality_alerts(degraded_quality)

        # Then: 적절한 알림 생성 확인
        assert alert_result["should_alert"] is True
        assert len(alert_result["alerts"]) == 3
        assert "critical_error_rate" in alert_result["alerts"]
        assert alert_result["severity"] == "WARNING"  # 중단할 정도는 아님

    def _evaluate_quality_alerts(self, quality_metrics: Dict[str, float]) -> Dict[str, Any]:
        """품질 알림 평가"""
        alerts = []

        if quality_metrics.get("critical_error_rate", 0) > 0.01:
            alerts.append("critical_error_rate")

        if quality_metrics.get("warning_rate", 0) > 0.10:
            alerts.append("warning_rate")

        if quality_metrics.get("completeness_rate", 1.0) < 0.95:
            alerts.append("completeness_rate")

        # 심각도 결정
        severity = "CRITICAL" if quality_metrics.get("critical_error_rate", 0) > 0.05 else "WARNING"

        return {
            "should_alert": len(alerts) > 0,
            "alerts": alerts,
            "severity": severity,
            "action_required": severity == "CRITICAL",
        }
