# Load Pipeline — Data Contracts

| 항목 | 값 |
|------|-----|
| 목적 | Curated S3 이벤트가 Load Event Publisher Lambda로 전달되는 데이터 구조 정의 |
| 코드 기준 | `infrastructure/pipelines/load/load_pipeline_stack.py`, `src/lambda/functions/load_event_publisher/handler.py` |
| 출력 소비자 | 도메인별 Load SQS 큐 (`{env}-{domain}-load-queue`) |

### S3 EventBridge 이벤트 구조

| 필드 | 타입 | 필수 | 설명 |
|------|------|:---:|------|
| `source` | string | ✅ | `"aws.s3"` |
| `detail-type` | string | ✅ | `"Object Created"` |
| `detail.bucket.name` | string | ✅ | Curated 버킷 이름 |
| `detail.object.key` | string | ✅ | Parquet 객체 키 |
| `detail.object.size` | integer | ✅ | 바이트 크기 (`>= MIN_FILE_SIZE_BYTES`) |
| `detail.object.etag` | string | ❌ | 객체 ETag |

### Curated 객체 키 규칙

| 세그먼트 | 예시 | 설명 |
|----------|------|------|
| `domain` | `market` | 도메인 |
| `table` | `prices` | 테이블 |
| `partition` | `ds=YYYY-MM-DD` | 날짜 파티션 (현행 Load 파서 기대값) |
| `object` | `part-0000.parquet` | Parquet 파일 |

### SQS 메시지 본문 및 속성

- 메시지 본문 스키마와 메시지 속성 정의는 `docs/specs/load/load-component-contracts.md`를 단일 소스로 사용합니다.
- Load Event Publisher Lambda는 해당 계약에 따라 메시지를 생성하며, Loader는 동일한 구조를 기대합니다.

> Transform 출력이 `interval/.../layer=...` 구조인 경우 Load 파서(`ds=` 기대)를 맞추도록 Lambda 전 처리 또는 Transform 경로 정합화가 필요합니다.
