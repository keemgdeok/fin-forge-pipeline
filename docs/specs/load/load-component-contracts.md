# Load Component Contracts (Pull Model)

| 항목 | 내용 |
|------|------|
| 목적 | Load Event Publisher Lambda, SQS 메시지 계약, 온프레미스 로더 요구사항 정의 |
| 코드 기준 | `src/lambda/functions/load_event_publisher/handler.py`, `src/lambda/shared/layers/core/python/load_contracts.py` |

## SQS 메시지 본문 계약

| Field | Type | Required | Format/Validation | Example |
|-------|------|:--------:|------------------|---------|
| `bucket` | string | ✅ | S3 bucket name | `data-pipeline-curated-dev` |
| `key` | string | ✅ | 기대 패턴 `domain/table/ds=YYYY-MM-DD/part-*.parquet` | `market/prices/ds=2025-09-10/part-001.parquet` |
| `domain` | string | ✅ | `[a-z0-9-]{1,50}` | `market` |
| `table_name` | string | ✅ | `[a-z0-9_]{1,50}` | `prices` |
| `partition` | string | ✅ | `ds=YYYY-MM-DD` | `ds=2025-09-10` |
| `file_size` | integer | ❌ | > 0 (있을 경우) | `1048576` |
| `correlation_id` | string | ✅ | UUID v4 | `550e8400-e29b-41d4-a716-446655440000` |
| `presigned_url` | string | ❌ | HTTPS URL | `https://...` |

| 주의 | 설명 |
|------|------|
| 경로 정합성 | Transform 출력은 `interval/.../layer=...` 구조이므로, Load 활성화 전에 키 파싱을 정합화해야 함 |
| presigned_url | Lambda에서 기본 제공하지 않으나, 보안 강화를 위해 확장 가능 |

## 메시지 속성

| Attribute | Type | Value 예시 | Purpose |
|-----------|------|------------|---------|
| `ContentType` | String | `application/json` | 메시지 본문 형식 |
| `Domain` | String | `market` | 도메인 라우팅 |
| `TableName` | String | `prices` | 테이블 라우팅 |
| `Priority` | String | `"1"` | 환경 설정 기반 우선순위 |

## 큐 구성 요약

| Setting | Main Queue | Dead Letter Queue | 설명 |
|---------|------------|-------------------|------|
| Name Pattern | `<env>-<domain>-load-queue` | `<env>-<domain>-load-dlq` | 환경/도메인 스코프 |
| Visibility Timeout | `visibility_timeout_seconds` (기본 1800초) | 기본값 30초 | 메시지 재처리 시간 |
| Message Retention | `message_retention_days` (기본 14일) | 14일 | 장기 보관 |
| Max Receive Count | `max_receive_count` (기본 3) | ∞ | 초과 시 DLQ 이동 |

## LoaderConfig 요구사항 (`load_contracts.LoaderConfig`)

| Parameter | Type | Required | Default | Validation |
|-----------|------|:--------:|---------|------------|
| `queue_url` | string | ✅ | - | SQS HTTPS URL 패턴 |
| `wait_time_seconds` | integer | ✅ | `20` | 1–20 |
| `max_messages` | integer | ✅ | `10` | 1–10 |
| `visibility_timeout` | integer | ✅ | `1800` | `>= query_timeout × 6` |
| `query_timeout` | integer | ✅ | `300` | > 0 |
| `backoff_seconds` | iterable[int] | ❌ | `[2,4,8]` | 양의 정수 배열 |

## Loader 메시지 처리 흐름

| 단계 | 필수 API | 동작 |
|------|----------|------|
| 폴링 | `ReceiveMessage` | 최대 10건, long poll |
| 성공 처리 | `DeleteMessage` (또는 Batch) | 메시지 삭제 |
| 일시 오류 | `ChangeMessageVisibility` | 백오프/연장 |
| 재시도 초과 | 자동 DLQ 이동 | `{env}-{domain}-load-dlq` |

## ClickHouse 테이블 계약 예시

| Column | Type | Nullable | 설명 |
|--------|------|:--------:|------|
| `domain` | String | ❌ | 도메인 |
| `table_name` | String | ❌ | 테이블 이름 |
| `partition_date` | Date | ❌ | `ds` 값 |
| `file_path` | String | ❌ | S3 키 |
| `record_count` | UInt64 | ❌ | 레코드 수 |
| `file_size` | UInt64 | ❌ | 바이트 수 |
| `correlation_id` | String | ❌ | 추적 UUID |
| `processed_at` | DateTime | ❌ | 적재 시각 |

## 보안 및 네트워크 요구사항

| 구성 요소 | 권한/설정 |
|-----------|-----------|
| Lambda (Publisher) | 대상 SQS 큐에 `sqs:SendMessage` 권한 |
| Loader | `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:ChangeMessageVisibility`, `s3:GetObject` |
| 네트워크 | 온프레미스 → AWS outbound (HTTPS 443). VPC 엔드포인트 선택적 |
| 암호화 | SQS/S3 SSE-S3 또는 KMS, 통신은 TLS 1.2+ |

## 미구현/추가 과제

| 항목 | 설명 |
|------|------|
| Loader 구현 | 저장소에 포함되지 않음. ClickHouse 적재 로직 및 재시도 전략 별도 개발 필요 |
| 경로 정합성 | Transform 출력 경로와 Load 파서 기대 경로(`ds=`) 일치화 필요 |
| 테스트 | `load_event_publisher` 및 계약 검증용 테스트 케이스 추가 필요 |

---

| 관련 문서 | 경로 |
|----------|------|
| Load Pipeline Spec | `docs/specs/load/load-pipeline-spec.md` |
| Publisher Lambda | `src/lambda/functions/load_event_publisher/handler.py` |
