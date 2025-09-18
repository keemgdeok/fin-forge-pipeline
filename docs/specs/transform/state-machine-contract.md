# Step Functions Transform State Machine — I/O 계약 명세

본 문서는 Transform 상태 머신의 입력/출력 계약, 실패 코드, 재시도 정책을 정의합니다. 모든 예시는 UTC 기준이며, CDK가 생성한 리소스와 일관되게 유지해야 합니다.

## 입력(Inputs)

| 필드 | 타입 | 필수 | 예시 | 제약/설명 |
|---|---|:---:|---|---|
| environment | string | Y | `dev` | `dev\|stg\|prod` 중 하나 |
| domain | string | Y | `daily-prices-data` | 도메인 식별자 |
| table_name | string | Y | `orders` | 데이터셋/테이블 이름 (`table` 별칭 허용) |
| ds | string | C | `2025-09-07` | `YYYY-MM-DD` (UTC). `date_range`와 상호 배타(XOR) |
| date_range.start | string | C | `2025-09-01` | `YYYY-MM-DD` (UTC) |
| date_range.end | string | C | `2025-09-07` | `YYYY-MM-DD` (UTC), `start ≤ end` |
| reprocess | boolean | N | `false` | 기본 `false`. 재처리 시 멱등 락 무시하고 진행 |
| execution_id | string | N | `ext-req-123` | 상관키. 미제공 시 상태 머신이 생성 |
| catalog_update | string | N | `on_schema_change` | `on_schema_change|never|force`. 기본은 스키마 변경 감지 시에만 크롤러 실행 |
| source_bucket | string | C | `data-pipeline-raw-dev-1234` | S3 트리거 모드일 때 필수 |
| source_key | string | C | `market/prices/ingestion_date=2025-09-07/file.json` | S3 트리거 모드일 때 필수 |
| file_type | string | N | `json` | `json|csv|parquet`. S3 트리거 모드에서 권장 |

- 두 가지 입력 모드를 지원합니다.
  - 직접 모드: `ds` 또는 `date_range` 제공(XOR).
  - S3 트리거 모드: `source_bucket`/`source_key` 제공 → Preflight에서 `ds`를 도출.
- 호환성: `table_name`을 기본으로 사용하되, 레거시 입력 `table`도 허용합니다(내부적으로 `table_name`으로 합쳐 처리).
- 백필(Map) 사용 시 `date_range` 길이는 운영 정책 범위 내에서 제한(권장: ≤ 31일).

### Preflight 출력(요약)

| 필드 | 타입 | 예시 | 설명 |
|---|---|---|---|
| proceed | boolean | `true` | 진행 여부(멱등 스킵 시 `false`) |
| ds | string | `2025-09-07` | 도출된 파티션(UTC) |
| glue_args | object | `{ "--ds": "2025-09-07", ... }` | Glue StartJobRun 인자 집합 |

## 출력(Outputs)

단일 실행(ds)과 범위(Map) 모두를 포괄합니다.

| 필드 | 타입 | 예시 | 설명 |
|---|---|---|---|
| ok | boolean | `true` | 전체 실행 성공 여부 |
| correlationId | string | `daily-prices-data:orders:2025-09-07` | `domain:table:ds` 또는 생성된 실행 키 |
| domain | string | `daily-prices-data` | 입력 반사 |
| table_name | string | `orders` | 입력 반사 |
| partitions | array<string> | `["ds=2025-09-07"]` | 성공적으로 처리된 파티션 목록 |
| stats.rowCount | number | `123456` | 출력 레코드 수(합계) |
| stats.bytesWritten | number | `987654321` | Curated 총 바이트 |
| stats.fileCount | number | `24` | Curated 생성 파일 수 |
| glueRunIds | array<string> | `["jr_abcdef"]` | Glue StartJobRun ID 리스트(Map 포함) |

실패 시에는 `ok=false`와 함께 오류 페이로드를 반환합니다(아래 참조).

## 실패 코드 및 오류 페이로드

| code | 발생 지점 | 설명 | 재시도 |
|---|---|---|---|
| PRE_VALIDATION_FAILED | Preflight | 입력 누락/형식 불일치 등 | 지수 백오프 최대 2회 |
| IDEMPOTENT_SKIP | Preflight | 이미 처리/잠금 중 → 스킵 | 재시도 없음(정상 종료 취급 가능) |
| NO_RAW_DATA | Preflight | 대상 Raw 파티션 없음 | 재시도 없음 |
| GLUE_JOB_FAILED | Glue | 애플리케이션 오류/리소스 부족 | 최대 1회 재시도 |
| DQ_FAILED | Glue | 데이터 품질 치명 규칙 위반 | 재시도 없음(수정 후 재실행) |
| CRAWLER_FAILED | Crawler | 크롤러 실패/타임아웃 | 최대 2회 재시도(백오프) |
| SCHEMA_CHECK_FAILED | Pre/Post | 스키마 지문 계산/비교 실패 | 1회 재시도 |
| TIMEOUT | SFN/Glue | 전체/태스크 타임아웃 | 원인에 따라 1회 재시도 |
| UNEXPECTED_ERROR | 전체 | 알 수 없는 예외 | 1회 재시도 후 실패 |

오류 페이로드 예시:

```json
{
  "ok": false,
  "correlationId": "daily-prices-data:orders:2025-09-07",
  "error": {
    "code": "DQ_FAILED",
    "message": "null ratio exceeded for column price",
    "partition": "ds=2025-09-07"
  }
}
```

## 성공 페이로드 예시

```json
{
  "ok": true,
  "correlationId": "daily-prices-data:orders:2025-09-07",
  "domain": "daily-prices-data",
  "table": "orders",
  "partitions": ["ds=2025-09-07"],
  "stats": {"rowCount": 123456, "bytesWritten": 987654321, "fileCount": 24},
  "glueRunIds": ["jr_abcdef"]
}
```

## 재시도 정책(요약)

- Preflight: 지수 백오프(예: 2x) 최대 2회. 검증 실패/데이터 없음은 비재시도.
- Glue: 시스템/일시 오류 시 1회. DQ_FAILED는 비재시도.
- Schema check: 1회(일시 오류만)
- Crawler: 최대 2회, 백오프로 간격 증가.
- 전체 실행: 표준 타입, 태스크별 `Retry`와 공통 `Catch`로 실패 페이로드 구성.

## 크롤러 실행 게이팅

- 정책(`catalog_update`)에 따라 Glue Crawler 실행 여부를 결정합니다.
  - `never`: 실행 안 함
  - `force`: 항상 실행
  - `on_schema_change`: 스키마 지문(`_schema/latest.json`)과 이전 지문(`_schema/previous.json`)의 `hash` 비교로 변경 시에만 실행
- Preflight 출력의 `glue_args['--schema_fingerprint_s3_uri']`를 활용하여 비교 대상 경로를 해석합니다.

## 관측(선택)

- 최소 요약만 보존 가능: 성공/실패 카운트와 오류 코드 중심(PII 금지). 특정 도구 의존 없음.

## 멱등성/상관키

- `correlationId = domain:table:ds`(단일) 또는 `execution_id` 기반.
- 현재 구현: Curated 경로 `/<domain>/<table>/ds=<ds>/`에 출력 존재 시 스킵(멱등).
- 선택적 고도화: DynamoDB 기반 실행 락/상태 추적을 추가 가능.

## 보안

- 최소권한 원칙: 프리픽스 단위 S3 접근, 로깅 저장소/KMS 권한은 필요한 범위로 한정.
- Glue Catalog/Athena 권한은 IAM으로 관리합니다. 컬럼 수준 제한이 필요하면 Athena View/별도 테이블로 분리해 뷰 단위로 권한을 부여합니다.
