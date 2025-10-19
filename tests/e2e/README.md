# LocalStack E2E Test Guide

이 디렉터리의 테스트는 LocalStack 위에서 **ingestion → transform → load** 파이프라인을 통합 검증

## Requirements

- LocalStack에서 `s3`, `sqs`, `dynamodb`, `ssm`, `stepfunctions`, `lambda` 활성화
- 필요 시 다음 환경 변수 설정 (기본값은 `LocalStackConfig` 참고)
  - `LOCALSTACK_ENDPOINT`
  - `LOCALSTACK_INTERNAL_ENDPOINT`
  - `AWS_REGION` / `AWS_DEFAULT_REGION`

## Execution

```bash
pytest tests/e2e/
```

### Options

- `E2E_PIPELINE_LIGHT_MODE=1` : Step Functions 실행 없이 S3 산출물만 빠르게 검증
- `E2E_ENABLE_FULL_WORKFLOW=1` : 100개 매니페스트를 사용하는 무거운 워크플로 켜기
- `E2E_FULL_PIPELINE_MANIFESTS`, `E2E_WORKFLOW_LIGHT_MANIFESTS`, `E2E_WORKFLOW_FULL_MANIFESTS` : 매니페스트 개수 조정

## Utils

- `tests/e2e/utils/workflow.py` : Lambda 스텁/Step Functions 배포 헬퍼
- `tests/e2e/lambda_stubs/` : LocalStack 전용 Lambda 스텁 모듈
- `tests/e2e/utils/zipper.py` + `tests/unit/e2e/test_zipper.py` : Lambda 패키징 유틸과 검증 테스트

## Solve

- “LocalStack endpoint not reachable” 스킵 발생 시 LocalStack 가동 여부와 엔드포인트 값 확인
- Step Functions 실패 시 헬퍼가 수집한 `TaskFailed`, `MapIterationFailed`, `ExecutionFailed` 로그를 에러 메시지에서 확인
