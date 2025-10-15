# Load Component Contracts (Pull Model)

| 항목 | 내용 |
|------|------|
| 목적 | Load Event Publisher Lambda, SQS 메시지 계약, 온프레미스 로더 요구사항 명세 |
| 코드 기준 | `src/lambda/functions/load_event_publisher/handler.py`, `src/lambda/shared/layers/core/python/load_contracts.py` |

### SQS 메시지 본문 계약

| 필드 | 타입 | 필수 | 검증 규칙 | 예시 |
|------|------|:---:|-----------|------|
| `bucket` | string | ✅ | S3 버킷 이름 (`[a-z0-9.-]{3,63}`) | `data-pipeline-curated-dev` |
| `key` | string | ✅ | `domain/table/ds=YYYY-MM-DD/part-*.parquet` | `market/prices/ds=2025-09-10/part-001.parquet` |
| `domain` | string | ✅ | `[a-z0-9-]{1,50}` | `market` |
| `table_name` | string | ✅ | `[a-z0-9_]{1,50}` | `prices` |
| `partition` | string | ✅ | `ds=YYYY-MM-DD` | `ds=2025-09-10` |
| `correlation_id` | string | ✅ | UUID v4 | `550e8400-e29b-41d4-a716-446655440000` |
| `file_size` | integer | ❌ | 존재 시 > 0 | `1048576` |
| `presigned_url` | string | ❌ | `https://` 시작 URL | `https://signed-url` |


### 메시지 속성

| Attribute | 값 | 용도 |
|-----------|-----|------|
| `ContentType` | `application/json` | 메시지 본문 포맷 식별 |
| `Domain` | `<domain>` | 도메인 라우팅 |
| `TableName` | `<table_name>` | 테이블 라우팅 |
| `Priority` | `PRIORITY_MAP[domain]` | 우선순위 (기본: market=1, daily-prices-data=2, 기타=3) |

### LoaderConfig 요구사항 (`load_contracts.LoaderConfig`)

| Parameter | Type | 필수 | 기본값 | 검증 |
|-----------|------|:---:|---------|------|
| `queue_url` | string | ✅ | - | SQS HTTPS URL 패턴 준수 |
| `wait_time_seconds` | integer | ✅ | `20` | 1–20 |
| `max_messages` | integer | ✅ | `10` | 1–10 |
| `visibility_timeout` | integer | ✅ | `1800` | `>= query_timeout × 6` |
| `query_timeout` | integer | ✅ | `300` | > 0 |
| `backoff_seconds` | iterable[int] | ❌ | `[2,4,8]` | 양의 정수 배열, 비어 있으면 오류 |

### ClickHouse table contract example

| 컬럼 | 타입 | Nullable | 설명 |
|------|------|:--------:|------|
| `domain` | String | ❌ | 도메인 식별자 |
| `table_name` | String | ❌ | 테이블 이름 |
| `partition_date` | Date | ❌ | `ds` 값 |
| `file_path` | String | ❌ | S3 키 |
| `record_count` | UInt64 | ❌ | 레코드 수 |
| `file_size` | UInt64 | ❌ | 바이트 크기 |
| `correlation_id` | String | ❌ | 추적 UUID |
| `processed_at` | DateTime | ❌ | 적재 시각 |
