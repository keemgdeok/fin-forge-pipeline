# SQS Integration — Data Contracts

| 항목      | 값                                                              |
| --------- | --------------------------------------------------------------- |
| 목적      | Orchestrator ↔ Worker 간 SQS 메시지 구조 및 실패 응답 계약 정의 |
| 큐 리소스 | `{env}-ingestion-queue` / `{env}-ingestion-dlq`                 |

### SQS 메시지 본문 (JSON)

| 필드                 | 타입            | 필수 | 설명                             |
| -------------------- | --------------- | :--: | -------------------------------- |
| `data_source`        | string          |  ✅  | 데이터 공급자                    |
| `data_type`          | string          |  ✅  | 데이터 유형                      |
| `domain`             | string          |  ✅  | 비즈니스 도메인                  |
| `table_name`         | string          |  ✅  | 대상 테이블                      |
| `symbols`            | array\[string\] |  ✅  | 처리 대상 심볼 묶음              |
| `period`             | string          |  ✅  | 공급자 쿼리 기간                 |
| `interval`           | string          |  ✅  | 공급자 쿼리 주기                 |
| `file_format`        | string          |  ✅  | 워커 저장 포맷 (`json`/`csv`)    |
| `batch_id`           | string          |  ✅  | 배치 UUID                        |
| `batch_ds`           | string          |  ✅  | `YYYY-MM-DD`                     |
| `batch_total_chunks` | integer         |  ✅  | 전체 청크 수                     |
| `correlation_id`     | string          |  ❌  | 현재 메시지 본문에 포함하지 않음 |

메시지 속성(`MessageAttributes`)은 사용하지 않음 <br>
모든 데이터는 Body(JSON)로 전달

### SQS 이벤트 → Worker Lambda

| 필드                      | 타입   | 설명                               |
| ------------------------- | ------ | ---------------------------------- |
| `Records[].messageId`     | string | 메시지 ID, 부분 실패 보고에 사용   |
| `Records[].receiptHandle` | string | SQS 삭제용 핸들                    |
| `Records[].body`          | string | UTF-8 JSON (위 메시지 본문 스키마) |

### Worker 응답 (Partial Batch Failure)

| 필드                                 | 타입           | 설명                        |
| ------------------------------------ | -------------- | --------------------------- |
| `batchItemFailures`                  | array\<object> | 실패 메시지 식별자 목록     |
| `batchItemFailures[].itemIdentifier` | string         | 실패한 메시지의 `messageId` |

빈 배열을 반환하면 모든 메시지를 성공으로 간주하고 삭제 <br>
실패 목록이 있으면 해당 항목만 재전달되며, 응답이 없거나 예외가 발생하면 전체 배치가 재시도
