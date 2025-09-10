# Event-Driven Triggers — 이벤트 기반 트리거 명세

본 문서는 Extract 파이프라인의 이벤트 기반 트리거 시스템을 정의합니다. EventBridge 스케줄, S3 이벤트 알림, 그리고 수동 트리거를 포함한 모든 파이프라인 시작점을 다룹니다.

## 트리거 아키텍처

Extract 파이프라인은 다음 3가지 트리거 패턴을 지원합니다:

1. **Scheduled Trigger**: 정시 실행 (EventBridge Rule + Cron)
2. **Event-Driven Trigger**: 외부 이벤트 기반 (EventBridge Custom Events)  
3. **Manual Trigger**: 수동 실행 (Lambda Direct Invocation)

## 1. Scheduled Trigger (기본)

### EventBridge Rule 구성

**Rule Name**: `{environment}-customer-ingestion-schedule`  
**State**: ENABLED  
**Schedule Expression**: Cron 기반

| 환경 | Cron 표현식 | 실행 시간 | 설명 |
|------|-------------|-----------|------|
| **dev** | `0 22 * * ? *` | 매일 22:00 UTC | 07:00 KST (한국 시간) |
| **staging** | `0 21 * * ? *` | 매일 21:00 UTC | 06:00 KST |
| **prod** | `0 20 * * ? *` | 매일 20:00 UTC | 05:00 KST |

**Cron 표현식 형식**:
```
분 시 일 월 요일 년도
0  22 *  *  ?    *
```

### 스케줄 이벤트 페이로드

EventBridge Rule이 Orchestrator Lambda에 전달하는 기본 이벤트:

```json
{
  "version": "0",
  "id": "12345678-1234-1234-1234-123456789012",
  "detail-type": "Scheduled Event",
  "source": "aws.events",
  "account": "123456789012",
  "time": "2025-09-09T22:00:00Z",
  "region": "us-east-1",
  "detail": {},
  "resources": ["arn:aws:events:us-east-1:123456789012:rule/dev-customer-ingestion-schedule"]
}
```

**Transform된 Ingestion Event**:

CDK에서 `RuleTargetInput.from_object()`로 변환된 실제 페이로드:

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
  "trigger_type": "schedule",
  "trigger_time": "2025-09-09T22:00:00Z"
}
```

### 환경별 설정

**Configuration Override**:

```python
# infrastructure/config/environments/dev.py
INGESTION_CONFIG = {
    "schedule_expression": "cron(0 22 * * ? *)",
    "ingestion_symbols": ["AAPL", "MSFT", "GOOGL"],
    "ingestion_period": "1mo", 
    "ingestion_interval": "1d",
    "ingestion_file_format": "json"
}

# infrastructure/config/environments/prod.py  
INGESTION_CONFIG = {
    "schedule_expression": "cron(0 20 * * ? *)",
    "ingestion_symbols": [],  # SSM에서 로드
    "ingestion_period": "1mo",
    "ingestion_interval": "1d", 
    "ingestion_file_format": "json"
}
```

## 2. Event-Driven Trigger (확장)

### Custom EventBridge Events

외부 시스템에서 Extract 파이프라인을 즉시 실행해야 할 때 사용합니다.

#### 지원하는 이벤트 소스

| 소스 | Event Source | Detail Type | 용도 |
|------|--------------|-------------|------|
| **Transform Pipeline** | `extract.pipeline` | `Processing Complete` | Transform 완료 후 재수집 |
| **Symbol Management** | `symbol.universe` | `Universe Updated` | 새 심볼 추가 시 즉시 수집 |
| **Market Events** | `market.events` | `Market Close` | 장 마감 후 즉시 수집 |
| **Manual Operations** | `ops.manual` | `Adhoc Ingestion` | 운영팀의 수동 트리거 |

#### Custom Event 형식

**Transform Complete Event**:
```json
{
  "version": "0",
  "id": "87654321-4321-4321-4321-210987654321",
  "detail-type": "Processing Complete",
  "source": "transform.pipeline",
  "account": "123456789012", 
  "time": "2025-09-09T16:30:45Z",
  "region": "us-east-1",
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

**Symbol Universe Update Event**:
```json
{
  "version": "0", 
  "id": "11111111-2222-3333-4444-555555555555",
  "detail-type": "Universe Updated",
  "source": "symbol.universe",
  "account": "123456789012",
  "time": "2025-09-09T10:15:30Z",
  "region": "us-east-1",
  "detail": {
    "added_symbols": ["NVDA", "AMD"],
    "removed_symbols": ["XOM"],
    "effective_date": "2025-09-10",
    "immediate_ingestion": true
  }
}
```

### EventBridge Rule Patterns

**Pattern Matching**:

```json
{
  "source": ["transform.pipeline"],
  "detail-type": ["Processing Complete"],
  "detail": {
    "trigger_reingestion": [true]
  }
}
```

**Multi-Source Pattern**:
```json
{
  "source": ["transform.pipeline", "symbol.universe", "ops.manual"],
  "detail-type": [{
    "exists": true
  }]
}
```

## 3. Manual Trigger (운영)

### Direct Lambda Invocation

운영팀 또는 개발팀의 수동 실행을 위한 트리거입니다.

#### CLI를 통한 실행

**AWS CLI**:
```bash
aws lambda invoke \
  --function-name dev-customer-data-orchestrator \
  --payload '{"data_source":"yahoo_finance","symbols":["AAPL"],"period":"1d","correlation_id":"manual-001"}' \
  response.json
```

**CDK CLI** (권장):
```bash
# 개발 환경에서 테스트 실행
cdk deploy CustomerDataIngestionStack \
  --parameters TriggerIngestion=true \
  --parameters TestSymbols=AAPL,MSFT
```

#### Console을 통한 실행

**Lambda Console**:
1. AWS Lambda Console 접속
2. `dev-customer-data-orchestrator` 함수 선택
3. "Test" 탭에서 이벤트 생성:

```json
{
  "data_source": "yahoo_finance",
  "data_type": "prices",
  "domain": "market", 
  "table_name": "prices",
  "symbols": ["AAPL", "MSFT"],
  "period": "1d",
  "interval": "1d",
  "file_format": "json",
  "correlation_id": "manual-console-test",
  "dry_run": false
}
```

### 수동 실행 파라미터

| 파라미터 | 타입 | 필수 | 기본값 | 설명 |
|----------|------|:---:|--------|------|
| `correlation_id` | string | N | 자동 생성 | 추적용 상관관계 ID |
| `dry_run` | boolean | N | `false` | true 시 SQS 전송 없이 로그만 |
| `force_symbols` | boolean | N | `false` | true 시 외부 소스 무시 |
| `override_config` | object | N | `{}` | 환경 설정 재정의 |

**Override Config 예시**:
```json
{
  "override_config": {
    "chunk_size": 1,
    "sqs_send_batch_size": 1, 
    "worker_timeout": 600
  }
}
```

## 4. S3 Event Notifications (Transform 연계)

### S3 → EventBridge Integration

Raw 데이터 저장 완료 시 Transform 파이프라인 트리거를 위한 이벤트 발생.

#### S3 Event Configuration

**Event Types**:
- `s3:ObjectCreated:*`
- `s3:ObjectCreated:Put`
- `s3:ObjectCreated:Post`

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
      "value": ".json"
    }
  ]
}
```

#### S3 Event 형식

**Raw S3 Event**:
```json
{
  "Records": [
    {
      "eventVersion": "2.1",
      "eventSource": "aws:s3",
      "eventTime": "2025-09-09T14:45:30.000Z",
      "eventName": "ObjectCreated:Put",
      "s3": {
        "bucket": {
          "name": "data-pipeline-raw-dev-1234"
        },
        "object": {
          "key": "market/prices/ingestion_date=2025-09-09/data_source=yahoo_finance/symbol=AAPL/interval=1d/period=1mo/2025-09-09T14-30-45Z.json",
          "size": 12345
        }
      }
    }
  ]
}
```

**Transform EventBridge Event**:
```json
{
  "version": "0",
  "id": "s3-notification-001",
  "detail-type": "Object Created",
  "source": "aws.s3", 
  "account": "123456789012",
  "time": "2025-09-09T14:45:30Z",
  "region": "us-east-1",
  "detail": {
    "bucket": "data-pipeline-raw-dev-1234",
    "object": {
      "key": "market/prices/ingestion_date=2025-09-09/...",
      "size": 12345
    }
  }
}
```

## 5. 트리거 조정 및 제어

### 중복 실행 방지

**Idempotency Keys**:
```python
# Orchestrator에서 생성
idempotency_key = f"{domain}:{table_name}:{ds}:{correlation_id}"

# DynamoDB에 저장하여 중복 실행 방지
def check_idempotency(key: str) -> bool:
    try:
        dynamodb.put_item(
            TableName=IDEMPOTENCY_TABLE,
            Item={'id': key, 'timestamp': int(time.time())},
            ConditionExpression='attribute_not_exists(id)'
        )
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            return False
        raise
```

### Circuit Breaker Pattern

**연속 실패 시 자동 중단**:

```python
class IngestionCircuitBreaker:
    def __init__(self, failure_threshold=5, timeout_seconds=3600):
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        
    def should_execute(self, domain: str, table_name: str) -> bool:
        failures = self.get_recent_failures(domain, table_name)
        return len(failures) < self.failure_threshold
        
    def record_failure(self, domain: str, table_name: str):
        # DynamoDB에 실패 기록
        pass
```

### Rate Limiting

**API 호출 제한**:

```python
# Yahoo Finance API 제한 고려
MAX_REQUESTS_PER_MINUTE = 100
MAX_SYMBOLS_PER_BATCH = 5

def calculate_delay(symbol_count: int) -> int:
    """API 제한을 고려한 지연 시간 계산"""
    return max(0, (symbol_count / MAX_REQUESTS_PER_MINUTE) * 60)
```

## 6. 모니터링 및 알람

### EventBridge 지표

| 지표 | 임계값 | 알람 조건 |
|------|--------|-----------|
| `SuccessfulInvocations` | - | 성공률 모니터링 |
| `FailedInvocations` | 1개 | > 임계값 5분간 |
| `MatchedRules` | - | 규칙 매칭 추적 |

### 커스텀 지표

**트리거별 실행 통계**:
```python
cloudwatch.put_metric_data(
    Namespace='Extract/Triggers',
    MetricData=[
        {
            'MetricName': 'TriggerExecution',
            'Value': 1,
            'Unit': 'Count',
            'Dimensions': [
                {'Name': 'TriggerType', 'Value': trigger_type},
                {'Name': 'Domain', 'Value': domain},
                {'Name': 'Status', 'Value': 'success'}
            ]
        }
    ]
)
```

### 대시보드

**CloudWatch Dashboard**:
- 스케줄 실행 현황
- 이벤트 드리븐 트리거 빈도
- 실패율 및 오류 패턴
- API 사용량 추적

## 7. 장애 처리 및 복구

### 스케줄 실행 실패

**자동 재시도**: EventBridge Rule에서 자동 재시도 없음  
**수동 복구**: 
1. CloudWatch Logs에서 실패 원인 확인
2. 수동 트리거로 재실행
3. 필요시 다음 스케줄까지 대기

### 이벤트 손실

**EventBridge DLQ**: 실패한 이벤트는 DLQ로 이동  
**모니터링**: DLQ 메시지 수 알람 설정  
**복구**: DLQ에서 이벤트 수동 재처리

### 스케줄 충돌

**동시 실행 방지**:
```python
# Lambda Reserved Concurrency = 1로 설정
# 또는 DynamoDB Lock 사용
```

## 8. 보안 고려사항

### IAM 권한

#### EventBridge Rule 권한 설정

| 구성 요소 | Principal | Action | Resource Pattern | 설명 |
|----------|-----------|--------|------------------|------|
| **EventBridge Rule** | `events.amazonaws.com` | `lambda:InvokeFunction` | `arn:aws:lambda:*:*:function:*-customer-data-orchestrator` | Lambda 함수 호출 권한 |
| **Lambda Function** | `lambda.amazonaws.com` | `events:PutEvents` | `arn:aws:events:*:*:event-bus/*` | EventBridge 이벤트 발행 권한 |

#### Cross-Account Events 권한 (향후 확장)

| 구성 요소 | Principal | Action | Resource Pattern | 용도 |
|----------|-----------|--------|------------------|------|
| **외부 계정** | `arn:aws:iam::OTHER-ACCOUNT:root` | `events:PutEvents` | `arn:aws:events:*:*:event-bus/custom-ingestion-bus` | 다른 AWS 계정에서 이벤트 발행 |
| **EventBridge Bus** | `events.amazonaws.com` | `lambda:InvokeFunction` | `arn:aws:lambda:*:*:function:*` | 크로스 계정 이벤트 처리 |

#### 최소 권한 원칙 적용

| 권한 카테고리 | 허용 범위 | 제한 사항 | 보안 고려사항 |
|-------------|-----------|-----------|---------------|
| **함수별 권한** | 특정 Lambda ARN만 | 와일드카드 사용 금지 | 함수명 패턴 검증 |
| **리소스별 권한** | 특정 EventBridge Bus만 | 계정 간 접근 제한 | Cross-account 신뢰 관계 |
| **액션별 권한** | 필요한 API만 | 관리 권한 제외 | 읽기/쓰기 분리 |

### Event Validation

#### 입력 검증 규칙

| 검증 항목 | 필수 여부 | 검증 조건 | 오류 코드 | 설명 |
|----------|:--------:|----------|-----------|------|
| **data_source** | ✅ | 1-50자, 영문+밑줄 | `INVALID_DATA_SOURCE` | 데이터 소스 식별자 |
| **domain** | ✅ | 1-50자, 영문+밑줄 | `INVALID_DOMAIN` | 비즈니스 도메인 |
| **table_name** | ✅ | 1-50자, 영문+밑줄 | `INVALID_TABLE_NAME` | 대상 테이블명 |
| **symbols** | ❌ | 배열, 1-100개 | `INVALID_SYMBOLS` | 심볼 목록 (선택적) |

#### Event 형식 검증

| 검증 레벨 | 검증 내용 | 실패 시 처리 | 로그 레벨 |
|----------|-----------|-------------|:---------:|
| **구조 검증** | JSON 형식, 필수 필드 존재 | 즉시 반환 | ERROR |
| **값 검증** | 데이터 타입, 범위, 형식 | 기본값 적용 | WARN |
| **비즈니스 검증** | 도메인 규칙, 참조 무결성 | 폴백 로직 | INFO |

#### Rate Limiting 설정

| 트리거 유형 | 제한 단위 | 제한값 | 초과 시 처리 | 모니터링 |
|------------|-----------|--------|-------------|----------|
| **Manual Trigger** | 시간당 | 10회 | HTTP 429 반환 | CloudWatch 지표 |
| **Event-Driven** | 분당 | 100회 | 큐잉 후 지연 처리 | 배치 크기 조정 |
| **Scheduled** | 제한 없음 | - | N/A | 실행 시간 모니터링 |

#### Validation Error 처리 매트릭스

| 오류 유형 | HTTP 코드 | 재시도 여부 | DLQ 이동 | 알람 레벨 |
|----------|:---------:|:-----------:|:--------:|:---------:|
| **구조 오류** | 400 | ❌ | ❌ | ERROR |
| **값 오류** | 400 | ❌ | ❌ | WARN |
| **Rate Limit** | 429 | ✅ (지연 후) | ❌ | WARN |
| **비즈니스 규칙 위반** | 422 | ❌ | ✅ | ERROR |

## 9. 운영 가이드

### 일반적인 운영 작업

#### AWS CLI 명령어 모음

| 작업 유형 | 명령어 | 파라미터 | 사용 시기 |
|----------|--------|----------|----------|
| **스케줄 일시 중단** | `aws events disable-rule` | `--name {rule-name}` | 정기 점검 시 |
| **스케줄 재활성화** | `aws events enable-rule` | `--name {rule-name}` | 점검 완료 후 |
| **스케줄 변경** | `aws events put-rule` | `--schedule-expression "cron(...)"` | 시간 조정 시 |
| **즉시 실행** | `aws lambda invoke` | `--payload '{...}'` | 긴급 상황 |

#### 스케줄 관리 명령어

```bash
# 스케줄 일시 중단
aws events disable-rule --name dev-customer-ingestion-schedule

# 스케줄 재활성화
aws events enable-rule --name dev-customer-ingestion-schedule

# 스케줄 시간 변경 (23시로 조정)
aws events put-rule \
  --name dev-customer-ingestion-schedule \
  --schedule-expression "cron(0 23 * * ? *)"
```

#### 긴급 실행 명령어

```bash
# 즉시 실행 (긴급)
aws lambda invoke \
  --function-name dev-customer-data-orchestrator \
  --payload '{"symbols":["AAPL"],"period":"1d","urgent":true}' \
  /tmp/response.json

# 실행 결과 확인
cat /tmp/response.json | jq '.'
```

### 문제 해결 가이드

#### 일반적인 문제 및 해결방법

| 문제 유형 | 증상 | 진단 방법 | 해결 방법 | 예상 해결 시간 |
|----------|------|-----------|-----------|:--------------:|
| **스케줄 미실행** | 정해진 시간에 실행 안됨 | EventBridge Rule 상태 확인 | Rule 활성화, 대상 확인 | 5분 |
| **Lambda 실행 실패** | 함수 오류 발생 | CloudWatch Logs 확인 | 코드 수정, 권한 조정 | 30분 |
| **권한 오류** | Access Denied 에러 | IAM 정책 검토 | 필요 권한 추가 | 15분 |
| **API 제한** | 429 Too Many Requests | API 호출 빈도 확인 | Rate Limiting 조정 | 60분 |

#### 진단 체크리스트

| 순서 | 확인 항목 | 명령어/도구 | 정상 상태 기준 |
|:----:|----------|------------|---------------|
| **1** | EventBridge Rule 상태 | `aws events describe-rule` | State: `ENABLED` |
| **2** | Lambda 함수 상태 | `aws lambda get-function` | State: `Active` |
| **3** | CloudWatch Logs | AWS Console | 오류 없는 실행 로그 |
| **4** | IAM 권한 | `aws iam simulate-principal-policy` | 모든 액션 `allowed` |
| **5** | API 응답 | 수동 API 호출 | HTTP 200 응답 |

#### 트러블슈팅 명령어 모음

```bash
# EventBridge Rule 상태 확인
aws events describe-rule --name dev-customer-ingestion-schedule

# Lambda 함수 최근 실행 로그 확인
aws logs filter-log-events \
  --log-group-name /aws/lambda/dev-customer-data-orchestrator \
  --start-time $(date -d '1 hour ago' +%s)000

# IAM 역할 정책 시뮬레이션
aws iam simulate-principal-policy \
  --policy-source-arn arn:aws:iam::ACCOUNT:role/ROLE_NAME \
  --action-names lambda:InvokeFunction
```

#### 에스컬레이션 매트릭스

| 문제 심각도 | 대응 시간 | 담당자 | 에스컬레이션 조건 |
|:-----------:|:---------:|--------|------------------|
| **P0** | 15분 | 온콜 엔지니어 | 전체 파이프라인 중단 |
| **P1** | 1시간 | 개발팀 리드 | 일부 기능 장애 |
| **P2** | 4시간 | 개발팀 | 성능 저하 |
| **P3** | 1일 | 운영팀 | 마이너 이슈 |

---

*본 명세는 `infrastructure/pipelines/customer_data/ingestion_stack.py`의 EventBridge 설정을 기반으로 작성되었습니다.*