# Extract Trigger Specification

| 구분 | 값 |
|------|-----|
| 문서 목적 | Extract 파이프라인의 트리거 진입점을 표 기반으로 명세 |
| 코드 기준 | `infrastructure/pipelines/daily_prices_data/ingestion_stack.py`, `src/step_functions/workflows/runner.py` |
| 적용 범위 | EventBridge 스케줄, 수동 실행, (미구현) Event 기반 트리거 |

## Trigger Pattern Matrix

| 패턴 | 구현 상태 | 입력 소스 | 후속 처리 | 비고 |
|------|-----------|-----------|-----------|------|
| Scheduled | ✅ (배포됨) | EventBridge Rule + Cron | Orchestrator Lambda | 환경별 기본 이벤트 사용 |
| Event-Driven | 🚧 (미구현) | EventBridge Custom Event | 구현 시 Step Functions 연계 필요 | `processing_triggers` 설정만 존재 |
| Manual | ⚙️ (직접 호출) | `aws lambda invoke` 등 | Orchestrator Lambda | 전용 파라미터 미구현 |

## Scheduled Trigger 정리

| 항목 | 값 |
|------|-----|
| 환경별 Cron | 환경 설정(`ingestion_trigger_type`, `cron`) | 
| 기본 이벤트 필드 | `data_source`, `data_type`, `domain`, `table_name`, `symbols`, `period`, `interval`, `file_format`, `trigger_type` |
| 심볼 로딩 순서 | SSM Parameter → S3 객체 → 이벤트 payload → 폴백 `["AAPL"]` |
| 배포 리소스 | EventBridge Rule → Lambda Target (`DailyPricesIngestionSchedule`) |

### 기본 이벤트 예시

| 필드 | 값 |
|------|-----|
| `data_source` | `yahoo_finance` |
| `data_type` | `prices` |
| `domain` | `market` |
| `table_name` | `prices` |
| `symbols` | `["AAPL", "MSFT"]` |
| `period` | `1mo` |
| `interval` | `1d` |
| `file_format` | `json` |
| `trigger_type` | `schedule` |

## Manual Trigger 요약

| 항목 | 값 |
|------|-----|
| 실행 주체 | 운영자/개발자 (CLI, Lambda 콘솔) |
| 필수 필드 | Scheduled 이벤트와 동일 (필요 시 오버라이드) |
| 미구현 항목 | `dry_run`, `force_symbols`, `override_config` |
| 호출 예시 | `aws lambda invoke --function-name <env>-daily-prices-data-orchestrator --payload '{"symbols":["AAPL"]}' resp.json` |

## Manifest Hand-off (S3 → Step Functions)

| 단계 | 설명 |
|------|------|
| 매니페스트 생성 | 워커가 `_batch.manifest.json`을 RAW 버킷에 업로드 |
| 자동 트리거 | **미배포** (EventBridge 규칙 없음) |
| 실행 책임 | 운영 스크립트 또는 `src/step_functions/workflows/runner.py`에서 `manifest_keys` 입력 생성 후 실행 |
| 필터 예시 (향후) | Prefix: `<domain>/`, Suffix: `.manifest.json`, Size ≥ 1KB |

## 미구현/추가 과제

| 항목 | 설명 |
|------|------|
| Event 기반 재실행 | `processing_triggers` 설정에 맞는 EventBridge 규칙과 Lambda 필요 |
| 자동 매니페스트 소비 | S3 이벤트 → Step Functions 실행을 위한 인프라 확장 필요 |
| 수동 실행 파라미터 | `dry_run`, `force_symbols`, `override_config` 지원 여부 결정 |

---

| 참고 문서 | 경로 |
|----------|------|
| Event 입력/출력 계약 | `docs/specs/extract/orchestrator-contract.md` |
| 매니페스트 처리 Runner | `src/step_functions/workflows/runner.py` |

## 4. S3 Event Notifications (Transform 연계)

### S3 Event Configuration

**Event Types**: `s3:ObjectCreated:*`

**Filter Rules**:
```json
{
  "filterRules": [
    {
      "name": "prefix",
      "value": "market/prices/"
    },
    {
      "name": "suffix", 
      "value": ".manifest.json"
    }
  ]
}
```

워커 Lambda는 하루치 배치가 완료되면 `_batch.manifest.json` 파일을 해당 파티션에 기록합니다. 기존에는 이 마커에 연결된 EventBridge 규칙이 Step Functions 실행을 직접 시작했지만, 현재는 EventBridge 연동을 제거하고 **단일 실행 기반으로 매니페스트 목록을 전달**합니다. 심볼별 RAW 객체는 그대로 저장되고, `scripts/validate/validate_pipeline.py` (또는 운영 오케스트레이터)가 DynamoDB 배치 트래커/S3 요약 정보를 읽어 하나의 상태 머신 실행 입력으로 변환합니다.

### Batch Tracker Coordination

SQS 워커는 동일한 배치에 대한 여러 청크를 병렬로 처리하므로, DynamoDB 기반의 `batch_tracker` 테이블이 추가되어 최종 청크만 매니페스트를 생성합니다. 오케스트레이터는 배치 시작 시 `expected_chunks`를 저장하고, 워커는 각 청크 완료 후 `processed_chunks`를 증가시킵니다. `processed_chunks == expected_chunks`가 되는 순간에만 상태를 `finalizing → complete`으로 전환하면서 매니페스트를 업로드하므로, 배치당 정확히 한 번만 S3 이벤트가 발생합니다.

### Transform 실행 입력 (단일 실행)

```json
{
  "manifest_keys": [
    {
      "ds": "2025-09-09",
      "manifest_key": "market/prices/interval=1d/data_source=yahoo_finance/year=2025/month=09/day=09/_batch.manifest.json",
      "source": "dynamodb"
    }
  ],
  "domain": "market",
  "table_name": "prices",
  "file_type": "json",
  "interval": "1d",
  "data_source": "yahoo_finance",
  "raw_bucket": "data-pipeline-raw-dev-123456789012",
  "catalog_update": "on_schema_change"
}
```

## 5. 오류 처리 및 제어

### 중복 실행 방지

```python
# Idempotency Key 생성
idempotency_key = f"{domain}:{table_name}:{ds}:{correlation_id}"

# DynamoDB에 저장하여 중복 실행 방지
def check_idempotency(key: str) -> bool:
    # DynamoDB PutItem with ConditionExpression
    # attribute_not_exists(id) 조건으로 중복 방지
```

### Circuit Breaker Pattern

| 조건 | 임계값 | 동작 | 복구 시간 |
|------|--------|------|----------|
| **연속 실패** | 5회 | 자동 중단 | 1시간 |
| **API Rate Limit** | 100회/분 | 지연 처리 | 동적 조정 |
| **DLQ 증가** | 50개 | 알람 발생 | 수동 처리 |

## 6. 모니터링

### EventBridge 지표

| 지표 | 임계값 | 알람 조건 |
|------|--------|-----------|
| `SuccessfulInvocations` | - | 성공률 모니터링 |
| `FailedInvocations` | 1개 | > 임계값 5분간 |
| `MatchedRules` | - | 규칙 매칭 추적 |

### 커스텀 지표

```python
# 트리거별 실행 통계
cloudwatch.put_metric_data(
    Namespace='Extract/Triggers',
    MetricData=[{
        'MetricName': 'TriggerExecution',
        'Value': 1,
        'Dimensions': [
            {'Name': 'TriggerType', 'Value': trigger_type},
            {'Name': 'Domain', 'Value': domain},
            {'Name': 'Status', 'Value': 'success'}
        ]
    }]
)
```

## 7. 장애 복구

### 주요 장애 시나리오 및 대응

| 시나리오 | 감지 시간 | 복구 시간 | 대응 절차 |
|----------|:--------:|:--------:|----------|
| **Worker 전체 중단** | 5분 | 30분 | 1) Lambda 로그 확인 2) IAM 점검 3) 수동 재배포 |
| **스케줄 실행 실패** | 10분 | 1시간 | 1) EventBridge Rule 확인 2) 수동 트리거로 재실행 |
| **메시지 무한 순환** | 30분 | 2시간 | 1) 특정 메시지 추적 2) DLQ 수동 이동 3) 코드 수정 |

### 복구 우선순위

| 우선순위 | 영향도 | 대응팀 | 에스컬레이션 시간 |
|:--------:|--------|--------|-------------------|
| **P0** | 전체 서비스 중단 | 온콜 엔지니어 | 15분 |
| **P1** | 부분 기능 장애 | 개발팀 | 1시간 |
| **P2** | 성능 저하 | 개발팀 | 4시간 |

## 8. 보안

### IAM 권한

| 구성 요소 | Principal | Action | Resource Pattern |
|----------|-----------|--------|------------------|
| **EventBridge Rule** | `events.amazonaws.com` | `lambda:InvokeFunction` | `arn:aws:lambda:*:*:function:*-orchestrator` |
| **Lambda Function** | `lambda.amazonaws.com` | `events:PutEvents` | `arn:aws:events:*:*:event-bus/*` |

### Event Validation

| 검증 항목 | 검증 조건 | 실패 시 처리 | 로그 레벨 |
|----------|----------|-------------|:---------:|
| **구조 검증** | JSON 형식, 필수 필드 존재 | 즉시 반환 | ERROR |
| **값 검증** | 데이터 타입, 범위, 형식 | 기본값 적용 | WARN |
| **비즈니스 검증** | 도메인 규칙, 참조 무결성 | 폴백 로직 | INFO |

---

*본 명세는 `infrastructure/pipelines/daily_prices_data/ingestion_stack.py`의 EventBridge 설정을 기반으로 작성되었습니다.*
