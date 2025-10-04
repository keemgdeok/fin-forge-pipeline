# SQS Integration — 메시지 큐 통합 명세

| 항목 | 내용 |
|------|------|
| 역할 | Orchestrator ↔ Worker 사이의 비동기 메시지 버퍼 |
| 코드 기준 | `infrastructure/pipelines/daily_prices_data/ingestion_stack.py` |
| 큐 구조 | `{env}-ingestion-queue` (Main) / `{env}-ingestion-dlq` (DLQ) |

## 큐 설정

| 설정 항목 | Main Queue | Dead Letter Queue | 비고 |
|----------|------------|-------------------|------|
| Queue Type | Standard | Standard | 순서 보장 불필요 |
| Visibility Timeout | `worker_timeout × 6` (Dev 1800초) | 300초 | 재전달 간격 제어 |
| Message Retention | 4일 | 14일 | DLQ는 장기 보존 |
| Receive Wait Time | 0초 | 0초 | Lambda 이벤트 매핑 기본값 |
| Max Receive Count | 환경 설정 `max_retries` (Dev 5) | ∞ | 초과 시 DLQ 이동 |
| Max Message Size | 256KB | 256KB | SQS 기본 제한 |
| KMS Encryption | 미사용 | 미사용 | 필요 시 별도 구성 |

### 환경별 파라미터 요약

| 환경 | Queue Timeout | SQS Batch Size | DLQ 알람 임계값 |
|------|---------------|----------------|----------------|
| dev | 1800초 | 1 | 100 메시지 |
| staging | 1800초 | 2 | 환경 설정값 |
| prod | 1800초 | 2 | 환경 설정값 |

## 메시지 계약

### SQS Message Body

| 필드 | 타입 | 필수 | 검증 규칙/설명 | 기본값 |
|------|------|:---:|----------------|--------|
| `data_source` | string | ✅ | 영문/숫자/언더스코어 | - |
| `data_type` | string | ✅ | 소문자, 언더스코어 | - |
| `domain` | string | ✅ | 소문자, 하이픈 | - |
| `table_name` | string | ✅ | 소문자, 언더스코어 | - |
| `symbols` | array | ✅ | 각 심볼 1–20자 | - |
| `period` | string | ❌ | 입력 그대로 전달 | `1mo` |
| `interval` | string | ❌ | 입력 그대로 전달 | `1d` |
| `file_format` | string | ❌ | `json` 또는 `csv` | `json` |
| `batch_id` | string | ✅ | UUID v4 | - |
| `batch_ds` | string | ✅ | `YYYY-MM-DD` | - |
| `batch_total_chunks` | integer | ✅ | > 0 | - |

### Message Attributes

| Attribute | 타입 | 값 예시 | 목적 |
|-----------|------|---------|------|
| `ContentType` | String | `application/json` | 메시지 포맷 식별 |
| `Domain` | String | `market` | 큐 라우팅 메타 |
| `TableName` | String | `prices` | 큐 라우팅 메타 |
| `Priority` | String | `1` | 도메인 우선순위 (config 기반) |

## 배치 전송/소비

| 구분 | 파라미터 | 값 | 비고 |
|------|----------|-----|------|
| Orchestrator → SQS | Batch Size | `sqs_send_batch_size` (최대 10) | SQS API 제한 준수 |
| Orchestrator → SQS | Retry | 1회 | 실패 항목만 재전송 |
| SQS → Worker | Batch Size | `sqs_batch_size` (Dev 1) | 부분 실패 격리 |
| SQS → Worker | Max Batching Window | 0초 | 실시간 처리 우선 |
| SQS → Worker | Parallelization Factor | 1 | 기본값 유지 |
| SQS → Worker | Partial Failure | 활성화 | 실패 메시지만 재전달 |

## 재시도 및 DLQ

| 단계 | 동작 | 설정 |
|------|------|------|
| Lambda 실행 실패 | 메시지 가시성 만료 후 재전달 | `worker_timeout × 6` |
| 재시도 한도 | `receiveCount > max_retries` | Dev 기준 5회 |
| DLQ 이동 | `{env}-ingestion-dlq` | 메시지 14일 보관 |

## 모니터링

| 알람 이름 | 지표 | 임계값 (Dev) | 정의 위치 |
|-----------|------|--------------|------------|
| IngestionQueueDepthAlarm | `ApproximateNumberOfMessagesVisible` | 100 메시지 | `ingestion_stack.py` |
| IngestionQueueAgeAlarm | `ApproximateAgeOfOldestMessage` | 300초 | `ingestion_stack.py` |

DLQ에 대한 알람은 기본 미제공이므로, 운영 요구에 따라 추가 구성해야 합니다.

---

| 관련 문서 | 경로 |
|----------|------|
| Orchestrator 계약 | `docs/specs/extract/orchestrator-contract.md` |
| Worker 계약 | `docs/specs/extract/worker-contract.md` |
| Raw 스키마 | `docs/specs/extract/raw-data-schema.md` |
