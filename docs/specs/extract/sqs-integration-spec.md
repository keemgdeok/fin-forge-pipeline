# SQS Integration — 메시지 큐 통합 명세

본 문서는 Extract 파이프라인에서 사용하는 Amazon SQS 큐 설계, 메시지 형식, 배치 처리, 그리고 재시도 정책을 정의합니다. SQS는 Orchestrator Lambda와 Worker Lambda 간의 비동기 통신 계층을 제공합니다.

## 큐 아키텍처

Extract 파이프라인은 **Main Queue + Dead Letter Queue** 패턴을 사용합니다.

| 큐 타입 | 이름 패턴 | 용도 | 라이프사이클 |
|---------|-----------|------|--------------|
| **Main Queue** | `{environment}-ingestion-queue` | 정상 메시지 처리 | 14일 보관 |
| **Dead Letter Queue** | `{environment}-ingestion-dlq` | 실패 메시지 격리 | 14일 보관 |

## 큐 설정 명세

| 설정 항목 | Main Queue | Dead Letter Queue | 근거 |
|----------|------------|-------------------|------|
| **Queue Type** | Standard | Standard | 순서 보장 불필요 |
| **Visibility Timeout** | 1800초 (30분) | 300초 (5분) | Worker 처리시간 vs 수동 처리 |
| **Message Retention** | 14일 | 14일 | AWS 기본값 유지 |
| **Receive Wait Time** | 0초 (단시간 폴링) | 20초 (장시간 폴링) | Lambda 최적화 vs 수동 처리 |
| **Max Receive Count** | 5회 | ∞ | DLQ 이동 조건 vs 영구 보관 |
| **Max Message Size** | 256KB | 256KB | SQS 기본 제한 |
| **KMS Encryption** | ✅ (기본키) | ✅ (기본키) | 저장 데이터 암호화 |

### 환경별 설정

| 환경 | Main Queue Timeout | DLQ 알람 임계값 | Batch Size | 근거 |
|------|-------------------|-----------------|------------|------|
| **dev** | 1800초 | 10개 | 1-5 | 디버깅 편의성 |
| **staging** | 1800초 | 5개 | 5-10 | 운영 환경 유사 |
| **prod** | 1800초 | 1개 | 10 | 최대 성능 + 즉시 알람 |

## 메시지 형식

### 메시지 구조

| 구성 요소 | 크기 제한 | 형식 | 설명 |
|----------|-----------|------|------|
| **Message Body** | 256KB | JSON 문자열 | 실제 페이로드 데이터 |
| **Message Attributes** | 10개, 각 256B | Key-Value | 메타데이터 (선택) |
| **Message ID** | - | UUID | SQS 자동 생성 |
| **Receipt Handle** | - | String | 메시지 삭제용 핸들 |

### Message Body 필드

| 필드 | 타입 | 필수 | 검증 규칙 | 기본값 |
|------|------|:---:|----------|--------|
| **data_source** | string | ✅ | 영문, 숫자, 언더스코어 | - |
| **data_type** | string | ✅ | 소문자, 언더스코어 | - |
| **domain** | string | ✅ | 소문자, 하이픈 | - |
| **table_name** | string | ✅ | 소문자, 언더스코어 | - |
| **symbols** | array | ✅ | 각 심볼 1-20자 | - |
| **period** | string | ❌ | Yahoo Finance 지원값 | `1mo` |
| **interval** | string | ❌ | Yahoo Finance 지원값 | `1d` |
| **file_format** | string | ❌ | json\|csv\|parquet | `json` |

## 배치 처리

### Orchestrator → SQS (SendMessageBatch)

| 파라미터 | 값 | 제한사항 | 최적화 전략 |
|----------|-----|----------|-------------|
| **Batch Size** | 1-10개 | SQS API 제한 | 항상 최대값 사용 |
| **Total Payload** | < 256KB | 모든 메시지 합계 | 메시지별 크기 사전 계산 |
| **Retry Logic** | 1회 재시도 | 실패 메시지만 재전송 | 부분 실패 지원 |

### SQS → Worker Lambda (ReceiveMessage)

| Event Source Mapping 설정 | 값 | 근거 |
|---------------------------|-----|------|
| **Batch Size** | 1-10 | 오류 격리 vs 처리량 |
| **Maximum Batching Window** | 0-5초 | 실시간성 vs 효율성 |
| **Parallelization Factor** | 10 | SQS 처리량 최적화 |
| **Report Batch Item Failures** | ✅ | Partial Failure 지원 |

## 재시도 및 오류 처리

### 재시도 흐름

| 단계 | 조건 | 액션 | Visibility Timeout | 대기 시간 |
|:---:|------|------|-------------------|-----------|
| **1** | 메시지 최초 수신 | Worker 처리 시도 | 1800초 | 즉시 |
| **2** | 처리 실패 (1회) | 큐로 반환 | 1800초 | 30분 |
| **3** | 처리 실패 (2회) | 큐로 반환 | 1800초 | 30분 |
| **4** | 처리 실패 (3회) | 큐로 반환 | 1800초 | 30분 |
| **5** | 처리 실패 (4회) | 큐로 반환 | 1800초 | 30분 |
| **6** | 처리 실패 (5회) | DLQ로 이동 | 영구 보관 | - |

### 오류 유형별 처리

| 오류 유형 | Receive Count | DLQ 이동 | 알람 | 자동복구 |
|-----------|:-------------:|:-------:|:----:|:-------:|
| **JSON 파싱 오류** | 5회 후 | ✅ | ❌ | ❌ |
| **네트워크 일시 장애** | 5회 후 | ✅ | ⚠️ | ❌ |
| **API Rate Limit** | 3회 후 | ✅ | ⚠️ | ❌ |
| **IAM 권한 오류** | 1회 후 | ✅ | 🚨 | ❌ |
| **Worker Lambda 오류** | 5회 후 | ✅ | 🚨 | ❌ |

## 모니터링

### CloudWatch 표준 지표

| 지표명 | 정상 범위 | 경고 임계값 | 위험 임계값 | 알람 지연 |
|--------|-----------|-------------|-------------|----------|
| **ApproximateNumberOfMessages** | 0-50 | > 100 | > 500 | 5분 |
| **ApproximateAgeOfOldestMessage** | 0-300초 | > 600초 | > 1800초 | 2분 |
| **NumberOfMessagesSent** | 10-1000/시간 | 급격한 변화 | 0 또는 10000+ | 10분 |
| **NumberOfMessagesDeleted** | 95%+ | < 90% | < 80% | 15분 |

### Dead Letter Queue 지표

| DLQ 지표 | 정상값 | 주의 임계값 | 위험 임계값 | 대응 시간 |
|----------|:-----:|-------------|-------------|:---------:|
| **Messages In DLQ** | 0 | > 5 | > 50 | 1시간 |
| **DLQ Growth Rate** | 0/시간 | > 2/시간 | > 10/시간 | 30분 |
| **DLQ Message Age** | - | > 6시간 | > 24시간 | 4시간 |

---

*본 명세는 `infrastructure/pipelines/customer_data/ingestion_stack.py`의 SQS 구성을 기반으로 작성되었습니다.*