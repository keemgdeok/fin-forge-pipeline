# Orchestrator Lambda — Data Contracts

| 항목      | 값                                                       |
| --------- | -------------------------------------------------------- |
| 목적      | Orchestrator Lambda 입력/출력 데이터 스키마 정의         |
| 코드 기준 | `src/lambda/functions/ingestion_orchestrator/handler.py` |
| 소비자    | SQS Ingestion Queue, DynamoDB 배치 트래커                |

## 입력 이벤트 페이로드

| 필드             | 타입            | 필수 | 기본값                           | 설명                                  |
| ---------------- | --------------- | :--: | -------------------------------- | ------------------------------------- |
| `data_source`    | string          |  ✅  | `yahoo_finance`                  | 데이터 공급자 식별자                  |
| `data_type`      | string          |  ✅  | `prices`                         | 데이터 유형                           |
| `domain`         | string          |  ✅  | `market`                         | 비즈니스 도메인                       |
| `table_name`     | string          |  ✅  | `prices`                         | 대상 테이블                           |
| `symbols`        | array\[string\] |  ❌  | `[]`                             | 비어 있으면 SSM/S3/기본값 순으로 로딩 |
| `period`         | string          |  ✅  | `1mo`                            | 공급자 쿼리 기간                      |
| `interval`       | string          |  ✅  | `1d`                             | 공급자 쿼리 주기                      |
| `file_format`    | string          |  ✅  | `json`                           | 워커 저장 포맷 (`json`/`csv`)         |
| `trigger_type`   | string          |  ❌  | 호출 패턴 (`schedule`, `manual`) |                                       |
| `batch_id`       | string          |  ❌  | `uuid4()` 생성                   | 사전 생성 배치 ID 사용 시 오버라이드  |
| `batch_ds`       | string          |  ❌  | 실행 날짜 (`YYYY-MM-DD`)         | 배치 기준 날짜 오버라이드             |
| `correlation_id` | string          |  ❌  | 없음                             | 로깅 상관 키                          |

## SQS 메시지 본문 (Worker 입력)

| 필드                 | 타입            | 필수 | 설명                               |
| -------------------- | --------------- | :--: | ---------------------------------- |
| `data_source`        | string          |  ✅  | 입력 이벤트 그대로 전달            |
| `data_type`          | string          |  ✅  | 입력 이벤트 그대로 전달            |
| `domain`             | string          |  ✅  | 입력 이벤트 그대로 전달            |
| `table_name`         | string          |  ✅  | 입력 이벤트 그대로 전달            |
| `symbols`            | array\[string\] |  ✅  | `CHUNK_SIZE` 기준 분할된 심볼 묶음 |
| `period`             | string          |  ✅  | 입력 이벤트 그대로 전달            |
| `interval`           | string          |  ✅  | 입력 이벤트 그대로 전달            |
| `file_format`        | string          |  ✅  | 입력 이벤트 그대로 전달            |
| `batch_id`           | string          |  ✅  | 배치 UUID                          |
| `batch_ds`           | string          |  ✅  | ISO 날짜 문자열                    |
| `batch_total_chunks` | integer         |  ✅  | 전체 청크 수                       |
| `correlation_id`     | string          |  ❌  | 입력에 존재할 경우 전달            |

> 메시지 속성(`MessageAttributes`)은 전송되지 않습니다.

## 배치 트래커 항목 (옵션)

| 필드               | 타입   | 설명                              |
| ------------------ | ------ | --------------------------------- |
| `pk`               | string | `batch_id` (파티션 키)            |
| `batch_ds`         | string | `YYYY-MM-DD`                      |
| `expected_chunks`  | number | 생성된 청크 수                    |
| `processed_chunks` | number | 초기값 `0`, 워커가 증가           |
| `status`           | string | 초기값 `processing`               |
| `environment`      | string | Lambda `ENVIRONMENT` 값           |
| `domain`           | string | 입력 `domain`                     |
| `table_name`       | string | 입력 `table_name`                 |
| `interval`         | string | 입력 `interval`                   |
| `data_source`      | string | 입력 `data_source`                |
| `ttl`              | number | Unix epoch (초), TTL 사용 시 설정 |

## Lambda 응답 페이로드

| 필드          | 타입    | 설명                 |
| ------------- | ------- | -------------------- |
| `published`   | integer | 발행된 SQS 메시지 수 |
| `chunks`      | integer | 생성된 청크 수       |
| `environment` | string  | Lambda 환경          |
| `batch_id`    | string  | 사용된 배치 UUID     |
| `batch_ds`    | string  | 배치 기준 날짜       |
