# Step Functions Transform State Machine — I/O 계약 명세

| 항목        | 내용                                                                             |
| ----------- | -------------------------------------------------------------------------------- |
| 책임        | 매니페스트 기반 배치를 Compaction → Transform → Indicators → Crawler 순으로 처리 |
| 코드 기준   | `infrastructure/pipelines/daily_prices_data/processing_stack.py`                 |
| 배포 리소스 | `{environment}-daily-prices-data-processing`                                     |

### 입력 계약

| 필드             | 타입          | 필수 | 기본값             | 설명                                                        |
| ---------------- | ------------- | :--: | ------------------ | ----------------------------------------------------------- |
| `manifest_keys`  | array<object> |  ✅  | -                  | 각 항목 `{ds, manifest_key, source?}`. Map 상태가 순차 처리 |
| `domain`         | string        |  ✅  | -                  | 도메인 식별자 (`table` 별칭도 허용)                         |
| `table_name`     | string        |  ✅  | -                  | 테이블 이름                                                 |
| `raw_bucket`     | string        |  ✅  | -                  | RAW S3 버킷                                                 |
| `file_type`      | string        |  ❌  | `json`             | Glue 인자에 사용                                            |
| `interval`       | string        |  ❌  | `1d`               | Glue 인자에 사용                                            |
| `data_source`    | string        |  ❌  | `yahoo_finance`    | Glue 인자에 사용                                            |
| `catalog_update` | string        |  ❌  | `on_schema_change` | `on_schema_change`/`never`/`force`                          |
| `environment`    | string        |  ❌  | 입력 없음          | Runner가 전달 가능 (참조용)                                 |
| `batch_id`       | string        |  ❌  | 입력 없음          | 외부 추적용 선택 필드                                       |

### Preflight 출력 요약

| 필드        | 타입    | 설명                                                     |
| ----------- | ------- | -------------------------------------------------------- |
| `proceed`   | boolean | `false` & `error.code=IDEMPOTENT_SKIP`이면 Map 항목 스킵 |
| `ds`        | string  | `manifest_keys` 항목에서 사용된 파티션 날짜              |
| `glue_args` | object  | Glue StartJobRun 공통 인수 집합                          |

#### `glue_args` 필드

| 키                                                   | 설명                                  |
| ---------------------------------------------------- | ------------------------------------- |
| `--raw_bucket`, `--raw_prefix`                       | RAW 입력 경로                         |
| `--compacted_bucket`, `--compacted_layer`            | 컴팩션 결과 버킷/레이어 (`compacted`) |
| `--curated_bucket`, `--curated_layer`                | 최종 결과 버킷/레이어 (`adjusted`)    |
| `--interval`, `--data_source`, `--file_type`, `--ds` | 파티션 지정                           |
| `--codec`, `--target_file_mb`                        | Glue Job 튜닝 파라미터                |
| `--schema_fingerprint_s3_uri`                        | 스키마 지문 경로                      |

### Map 처리 및 동시성

| 항목           | 값                                    | 설명                                  |
| -------------- | ------------------------------------- | ------------------------------------- |
| Map 상태       | `ProcessManifestList`                 | `manifest_keys` 배열 순회             |
| `items_path`   | `$.manifest_keys`                     | 각 항목 `{ds, manifest_key, source?}` |
| 최대 동시 실행 | `config.sfn_max_concurrency` (기본 1) | 배치별 순차 처리 보장 기본값          |

### 출력/오류 처리

| 항목          | 현행 동작                                                                    |
| ------------- | ---------------------------------------------------------------------------- |
| 성공 결과     | 모든 항목 처리 후 `Succeed` (추가 페이로드 없음)                             |
| 오류 페이로드 | `Fail` 상태로 즉시 종료, 상세는 CloudWatch Logs 및 실행 히스토리 참고        |
| 멱등성        | Preflight가 Curated `layer=adjusted` 경로 존재 여부 확인 (`IDEMPOTENT_SKIP`) |

### 재시도 정책

| 단계                             | 재시도 조건                            | 정책                         |
| -------------------------------- | -------------------------------------- | ---------------------------- |
| Glue ETL                         | `Glue.ConcurrentRunsExceededException` | 구성된 backoff/attempts 사용 |
| Glue Compaction                  | 재시도 없음                            | 실패 시 Fail                 |
| Lambda (Preflight/Guard/Decider) | 재시도 없음                            | 실패 시 Fail                 |
| Glue Crawler                     | 재시도 없음                            | 실패 시 Fail                 |

### Crawler 게이팅

| 정책 값            | 실행 여부                | 비고                |
| ------------------ | ------------------------ | ------------------- |
| `never`            | 실행 안 함               | 운영자가 수동 관리  |
| `force`            | 항상 실행                | 비용/시간 증가 주의 |
| `on_schema_change` | 지문 `hash` 변경 시 실행 | 기본값              |
