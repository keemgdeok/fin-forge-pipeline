# Event-Driven Triggers — 이벤트 기반 트리거 명세

본 문서는 Extract 파이프라인의 이벤트 기반 트리거 시스템을 정의합니다. EventBridge 스케줄, S3 이벤트 알림, 그리고 수동 트리거를 포함한 모든 파이프라인 시작점을 다룹니다.

## 트리거 아키텍처

Extract 파이프라인은 다음 3가지 트리거 패턴을 지원합니다:

1. **Scheduled Trigger**: 정시 실행 (EventBridge Rule + Cron)
2. **Event-Driven Trigger**: 외부 이벤트 기반 (EventBridge Custom Events)  
3. **Manual Trigger**: 수동 실행 (Lambda Direct Invocation)

## 1. Scheduled Trigger (기본)

### EventBridge Rule 구성

| 환경 | Cron 표현식 | 실행 시간 | 설명 |
|------|-------------|-----------|------|
| **dev** | `0 22 ? * MON-FRI *` | 매주 월-금 22:00 UTC | 07:00 KST (거래일) |
| **staging** | `0 21 ? * MON-FRI *` | 매주 월-금 21:00 UTC | 06:00 KST |
| **prod** | `0 20 ? * MON-FRI *` | 매주 월-금 20:00 UTC | 05:00 KST |

### 기본 스케줄 이벤트 페이로드

```json
{
  "data_source": "yahoo_finance",
  "data_type": "prices", 
  "domain": "market",
  "table_name": "prices",
  "symbols": ["AAPL", "MSFT", "GOOGL", "AMZN"],
  "period": "1mo",
  "interval": "1d",
  "file_format": "json",
  "trigger_type": "schedule"
}
```

### 환경별 설정

| 환경 | 스케줄 | 심볼 소스 | 파일 형식 | 근거 |
|------|--------|----------|----------|------|
| **dev** | `cron(0 22 * * ? *)` | 고정값 `["AAPL", "MSFT"]` | `json` | 디버깅 편의성 |
| **staging** | `cron(0 21 * * ? *)` | SSM 파라미터 | `json` | 운영 환경 유사 |
| **prod** | `cron(0 20 * * ? *)` | SSM 파라미터 | `json` | 최대 성능 |

## 2. Event-Driven Trigger (확장)

### 지원하는 이벤트 소스

| 소스 | Event Source | Detail Type | 용도 |
|------|--------------|-------------|------|
| **Transform Pipeline** | `extract.pipeline` | `Processing Complete` | Transform 완료 후 재수집 |
| **Symbol Management** | `symbol.universe` | `Universe Updated` | 새 심볼 추가 시 즉시 수집 |
| **Market Events** | `market.events` | `Market Close` | 장 마감 후 즉시 수집 |
| **Manual Operations** | `ops.manual` | `Adhoc Ingestion` | 운영팀의 수동 트리거 |

### Transform Complete Event 예시

```json
{
  "version": "0",
  "detail-type": "Processing Complete",
  "source": "transform.pipeline",
  "detail": {
    "domain": "market",
    "table_name": "prices", 
    "partition": "ds=2025-09-09",
    "trigger_reingestion": true,
    "symbols": ["AAPL", "TSLA"],
    "reason": "data_quality_issue"
  }
}
```

### Symbol Universe Update Event 예시

```json
{
  "version": "0",
  "detail-type": "Universe Updated", 
  "source": "symbol.universe",
  "detail": {
    "added_symbols": ["NVDA", "AMD"],
    "removed_symbols": ["XOM"],
    "effective_date": "2025-09-10",
    "immediate_ingestion": true
  }
}
```

## 3. Manual Trigger (운영)

### CLI를 통한 실행

```bash
# AWS CLI
aws lambda invoke \
  --function-name dev-daily-prices-data-orchestrator \
  --payload '{"data_source":"yahoo_finance","symbols":["AAPL"],"correlation_id":"manual-001"}' \
  response.json

# CDK CLI (권장)
cdk deploy DailyPricesDataIngestionStack \
  --parameters TriggerIngestion=true \
  --parameters TestSymbols=AAPL,MSFT
```

### 수동 실행 파라미터

| 파라미터 | 타입 | 기본값 | 설명 |
|----------|------|--------|------|
| `correlation_id` | string | 자동 생성 | 추적용 상관관계 ID |
| `dry_run` | boolean | `false` | true 시 SQS 전송 없이 로그만 |
| `force_symbols` | boolean | `false` | true 시 외부 소스 무시 |
| `override_config` | object | `{}` | 환경 설정 재정의 |

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
