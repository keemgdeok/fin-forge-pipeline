# Worker Lambda — Data Contracts

| 항목 | 값 |
|------|-----|
| 목적 | Ingestion Worker Lambda의 입력/출력 데이터 구조와 산출물 명세 |
| 코드 기준 | `src/lambda/functions/ingestion_worker/handler.py`, `src/lambda/layers/common/python/shared/ingestion/service.py` |

### 입력: SQS 메시지 본문

| 필드 | 타입 | 필수 | 설명 |
|------|------|:---:|------|
| `data_source` | string | ✅ | 데이터 공급자 |
| `data_type` | string | ✅ | 데이터 유형 |
| `domain` | string | ✅ | 비즈니스 도메인 |
| `table_name` | string | ✅ | 대상 테이블 |
| `symbols` | array[string] | ✅ | 처리 대상 심볼 묶음 |
| `period` | string | ✅ | 공급자 쿼리 기간 |
| `interval` | string | ✅ | 공급자 쿼리 주기 |
| `file_format` | string | ✅ | 저장 포맷 (`json`/`csv`) |
| `batch_id` | string | ✅ | 배치 UUID |
| `batch_ds` | string | ✅ | `YYYY-MM-DD` |
| `batch_total_chunks` | integer | ✅ | 전체 청크 수 |
| `correlation_id` | string | ❌ | 전달 시 로깅에 사용 |

### 출력: Lambda 성공 응답 본문

| 필드 | 타입 | 설명 |
|------|------|------|
| `message` | string | `"Data ingestion completed"` |
| `data_source` | string | 입력 값 |
| `data_type` | string | 입력 값 |
| `symbols_requested` | array[string] | 입력 심볼 전체 |
| `symbols_processed` | array[string] | 실제 처리된 심볼 |
| `invalid_symbols` | array[string] | 필터링된 심볼 |
| `period` | string | 입력 값 |
| `interval` | string | 입력 값 |
| `domain` | string | 입력 값 |
| `table_name` | string | 입력 값 |
| `file_format` | string | 입력 값 |
| `environment` | string | Lambda 환경 변수 |
| `raw_bucket` | string | RAW 버킷 이름 |
| `processed_records` | integer | 적재된 레코드 수 |
| `written_keys` | array[string] | 생성된 S3 객체 키 목록 |
| `partition_summaries` | array\<object> | 매니페스트 생성을 위한 요약 |

#### `partition_summaries` 항목 구조

| 필드 | 타입 | 설명 |
|------|------|------|
| `ds` | string | `YYYY-MM-DD` 파티션 날짜 |
| `raw_prefix` | string | RAW 파티션 프리픽스 (`domain/.../day=DD/`) |
| `objects` | array\<object> | 청크 결과 목록 |
| `objects[].symbol` | string | 심볼 |
| `objects[].key` | string | S3 객체 키 |
| `objects[].records` | integer | 객체 내 레코드 수 |

### 배치 트래커 업데이트 (DynamoDB)

| 필드 | 타입 | 설명 |
|------|------|------|
| `processed_chunks` | number | 메시지 처리 시 +1 |
| `last_update` | string | ISO 8601 타임스탬프 |
| `partition_payload` | list\<object> | `{"ds", "raw_prefix", "object_count"}` 누적 |
| `combined_partition_summaries` | list\<object> | 최종 청크가 집계한 partition 요약 |
| `status` | string | `processing` → `finalizing` → `complete` (Worker가 전환) |

### 매니페스트 파일 (`_batch.manifest.json`)

| 필드 | 타입 | 설명 |
|------|------|------|
| `environment` | string | 실행 환경 |
| `domain` | string | 도메인 |
| `table_name` | string | 테이블 |
| `data_source` | string | 데이터 공급자 |
| `interval` | string | 수집 간격 |
| `ds` | string | 파티션 날짜 (`YYYY-MM-DD`) |
| `generated_at` | string | ISO 8601 생성 시각 |
| `objects` | array<object> | 매니페스트 대상 RAW 객체 |
| `objects[].symbol` | string | 심볼 |
| `objects[].key` | string | RAW 객체 키 |
| `objects[].records` | integer | 객체 내 레코드 수 |
| `batch_id` | string | 배치 UUID |

### SQS Partial Failure 응답

| 필드 | 타입 | 설명 |
|------|------|------|
| `batchItemFailures` | array\<object> | 실패 메시지 식별자 목록 |
| `batchItemFailures[].itemIdentifier` | string | 실패한 SQS `messageId` |
