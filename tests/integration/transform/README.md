# Transform Domain Integration Tests

Transform 도메인의 포괄적인 통합 테스트 스위트입니다. 실제 AWS 서비스 동작을 모킹하여 전체 파이프라인을 검증합니다.

## 📋 테스트 구조

### Core Integration Tests

| 테스트 파일                        | 목적                      | 주요 검증 사항                       |
| ---------------------------------- | ------------------------- | ------------------------------------ |
| `test_step_functions_workflow.py`  | Step Functions 워크플로우 | 상태 전이, 에러 핸들링, 백필 맵 처리 |
| `test_aws_service_integration.py`  | AWS 서비스 통합           | S3→EventBridge→SFN 트리거 체인       |
| `test_end_to_end_pipeline.py`      | E2E 파이프라인            | Raw→Curated 전체 데이터 플로우       |
| `test_glue_crawler_integration.py` | Glue Crawler              | 스키마 감지, 카탈로그 업데이트       |
| `test_workflow_robustness.py`      | 견고성 테스트             | 동시 실행, 복구, 대용량 처리         |

### Test Coverage Matrix

| 기능 영역       | Unit Tests  | Integration Tests | Coverage |
| --------------- | ----------- | ----------------- | -------- |
| ETL Logic       | ✅ 10 tests | ✅ 15 tests       | 95%      |
| Step Functions  | ❌ 0 tests  | ✅ 6 tests        | 80%      |
| AWS Integration | ❌ 0 tests  | ✅ 8 tests        | 85%      |
| Error Handling  | ✅ 5 tests  | ✅ 12 tests       | 90%      |
| Performance     | ❌ 0 tests  | ✅ 4 tests        | 70%      |

## 🚀 테스트 실행 방법

### 전체 통합 테스트 실행

```bash
# 모든 통합 테스트 실행
pytest tests/integration/transform/ -v

# 병렬 실행 (속도 향상)
pytest tests/integration/transform/ -v -n auto
```

### 특정 테스트 그룹 실행

```bash
# Step Functions 워크플로우만 테스트
pytest tests/integration/transform/test_step_functions_workflow.py -v

# 견고성 테스트만 실행
pytest tests/integration/transform/test_workflow_robustness.py -v

# End-to-End 파이프라인만 테스트
pytest tests/integration/transform/test_end_to_end_pipeline.py -v

# 느린(LocalStack/Spark 등) 통합 테스트까지 실행
pytest tests/integration/transform/ -v --runslow
```

### 마커 기반 실행

```bash
# 통합 테스트만 실행
pytest -m integration -v

# 견고성 테스트만 실행
pytest -m robustness -v

# 성능 테스트만 실행
pytest -m performance -v
```

### 특정 시나리오 테스트

```bash
# 동시 실행 테스트
pytest tests/integration/transform/test_workflow_robustness.py::TestWorkflowRobustness::test_concurrent_execution_idempotency -v

# 스키마 진화 테스트
pytest tests/integration/transform/test_end_to_end_pipeline.py::TestEndToEndPipeline::test_schema_evolution_pipeline -v

# 백필 처리 테스트
pytest tests/integration/transform/test_step_functions_workflow.py::TestStepFunctionsWorkflow::test_backfill_map_workflow -v
```

## 🔧 테스트 환경 설정

### 필수 Dependencies

```bash
# 테스트 의존성 설치
pip install pytest moto boto3 pandas pyarrow
pip install pytest-xdist pytest-mock pytest-cov

# 병렬 실행을 위한 추가 패키지
pip install pytest-parallel
```

### 환경 변수 설정

```bash
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=testing
export AWS_SECRET_ACCESS_KEY=testing

# 선택적: 로그 레벨 설정
export PYTHONPATH="${PYTHONPATH}:${PWD}/src"
```

## 🧪 주요 테스트 시나리오

### 1. Step Functions Workflow Tests

#### 정상 플로우 검증

- **Single Partition Processing**: Preflight → Glue → Crawler 체인
- **Backfill Map Processing**: 동시성 제한(MaxConcurrency=3) 검증
- **Idempotent Skip**: 중복 처리 방지 검증

#### 에러 시나리오 검증

- **Preflight Validation Failure**: 입력 검증 실패 처리
- **Glue Job Failure**: ETL 작업 실패 시 에러 핸들링
- **Data Quality Failure**: DQ 규칙 위반 시 격리 처리

### 2. AWS Service Integration Tests

#### 이벤트 트리거 체인

- **S3 → EventBridge → Step Functions**: 파일 업로드 트리거 검증
- **Multi-prefix/suffix Filtering**: 복잡한 이벤트 패턴 매칭
- **Cross-stack IAM Permissions**: 권한 체인 검증

#### 모니터링 통합

- **CloudWatch Logging**: Step Functions 로그 기록 검증
- **X-Ray Tracing**: 분산 추적 설정 검증

### 3. End-to-End Pipeline Tests

#### 데이터 변환 검증

- **JSON → Parquet**: 스키마 추론 및 타입 변환
- **CSV → Parquet**: 타입 명시적 변환 및 압축
- **Multi-partition Processing**: 백필 시나리오

#### 품질 관리 검증

- **Data Quality Enforcement**: 규칙 위반 시 격리
- **Schema Evolution**: 스키마 변경 감지 및 적응
- **Schema Fingerprinting**: 일관성 해시 생성

### 4. Glue Crawler Integration Tests

#### 카탈로그 관리

- **Table Creation**: 새 데이터 감지 시 테이블 생성
- **Schema Change Detection**: 스키마 변경 자동 감지
- **Partition Discovery**: 파티션 자동 등록

#### 최적화 검증

- **CRAWL_NEW_FOLDERS_ONLY**: 증분 크롤링 정책
- **Quarantine Exclusion**: 불량 데이터 제외 처리
- **Performance Optimization**: 대량 파티션 효율적 처리

### 5. Workflow Robustness Tests

#### 동시성 및 충돌 관리

- **Concurrent Execution**: 동시 실행 멱등성 보장
- **Resource Contention**: 리소스 경합 시 우선순위 처리

#### 복구 메커니즘

- **Partial Failure Recovery**: 부분 실패 시 재시도 로직
- **Data Consistency**: 중단 후 재시작 시 일관성 유지
- **Checkpoint/Resume**: 체크포인트 기반 복구

#### 성능 및 확장성

- **Large Dataset Processing**: 1GB+ 데이터 처리 성능
- **Resource Exhaustion**: 제한된 리소스에서 graceful degradation

## 📊 테스트 결과 분석

### Coverage Report 생성

```bash
# 커버리지 리포트 생성
pytest tests/integration/transform/ --cov=src/glue --cov=src/lambda/functions/preflight --cov-report=html

# 결과 확인
open htmlcov/index.html
```

### 성능 벤치마크

```bash
# 성능 테스트 실행 및 벤치마크
pytest tests/integration/transform/test_workflow_robustness.py::TestWorkflowRobustness::test_large_dataset_processing_performance -v -s

# 시간 측정 포함
pytest tests/integration/transform/ --durations=10
```

## 🔍 디버깅 및 트러블슈팅

### 상세 로그 활성화

```bash
# AWS 서비스 호출 로그 확인
pytest tests/integration/transform/ -v -s --log-cli-level=DEBUG

# 특정 테스트 디버그
pytest tests/integration/transform/test_step_functions_workflow.py::TestStepFunctionsWorkflow::test_single_partition_workflow_success -v -s --pdb
```

### 일반적인 문제 해결

#### 1. Mock 설정 문제

```python
# moto 버전 호환성 확인
pip list | grep moto

# AWS 자격증명 Mock 재설정
@pytest.fixture(autouse=True)
def reset_aws_credentials():
    import os
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    # ...
```

#### 2. 메모리 부족 (대용량 테스트)

```bash
# 메모리 사용량 모니터링
pytest tests/integration/transform/test_workflow_robustness.py -v --tb=short

# 테스트 데이터 크기 조정
# _generate_large_dataset(10000)  # 100K에서 10K로 줄임
```

#### 3. 테스트 격리 문제

```python
# 각 테스트 후 리소스 정리
@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    # S3 버킷 정리 로직
```

## 🚦 CI/CD 통합

### GitHub Actions 설정

```yaml
- name: Run Transform Integration Tests
  run: |
    pytest tests/integration/transform/ -v -x --tb=short

- name: Upload Test Results
  uses: actions/upload-artifact@v3
  with:
    name: transform-test-results
    path: test-results/
```

### 테스트 분류별 실행

```bash
# Critical path 테스트 (빠른 피드백)
pytest tests/integration/transform/test_step_functions_workflow.py tests/integration/transform/test_end_to_end_pipeline.py -v

# Full regression 테스트 (야간 실행)
pytest tests/integration/transform/ -v --maxfail=5
```

## 🎯 테스트 개선 로드맵

### Phase 1 완료 ✅

- Step Functions 워크플로우 통합 테스트
- AWS 서비스 간 통합 테스트
- End-to-end 파이프라인 검증
- Glue Crawler 통합 테스트
- 워크플로우 견고성 테스트

### Phase 2 (향후 계획)

- [ ] 실제 AWS 환경 대상 E2E 테스트
- [ ] 성능 벤치마킹 자동화
- [ ] 멀티 리전 배포 테스트
- [ ] 보안 침투 테스트

### Phase 3 (장기 계획)

- [ ] Chaos Engineering 테스트
- [ ] ML 기반 이상 탐지 테스트
- [ ] Cost optimization 검증 테스트

## 🤝 기여 가이드

새로운 테스트 추가 시:

1. **테스트 명명 규칙**: `test_<시나리오>_<예상결과>.py`
1. **마커 태그**: `@pytest.mark.integration` 필수
1. **문서화**: Docstring에 Given-When-Then 형식 사용
1. **픽스처 재사용**: 공통 설정은 `conftest.py` 활용
1. **에러 처리**: 예상 실패 시나리오도 테스트에 포함

예시:

```python
@pytest.mark.integration
def test_new_feature_success_scenario(self, aws_credentials, test_config):
    """
    Given: 새로운 기능이 활성화되고 정상적인 입력이 있으면
    When: 파이프라인을 실행하면
    Then: 예상된 결과가 생성되어야 함
    """
    # 테스트 구현
```
