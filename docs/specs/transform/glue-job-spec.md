# Glue Job 명세 (표 형식)

다음 명세는 `<domain>/<table>` 변환 잡의 계약을 문서화합니다. 값은 환경/도메인별로 치환하세요.

## 기본 정보

| 항목 | 값 |
|---|---|
| 잡 이름 | `<domain>-<table>-transform` |
| Glue 버전 | 4.0 또는 5.0 |
| 언어 | Python (PySpark/PyGlue) |
| 워커 타입 | G.1X |
| DPU (최소/최대) | 2 / 2 (오토스케일 off, 고정) |
| 타임아웃(분) | 30 |
| 재시도 횟수 | 1 |

## 파라미터(Arguments)

| 키 | 예시 | 설명 |
|---|---|---|
| `--ds` | `2025-09-07` | 처리 대상 파티션(UTC) |
| `--raw_bucket` | `my-raw-bucket` | 입력 버킷 |
| `--raw_prefix` | `<domain>/<table>/` | 입력 프리픽스 |
| `--curated_bucket` | `my-curated-bucket` | 출력 버킷 |
| `--curated_prefix` | `<domain>/<table>/` | 출력 프리픽스 |
| `--job-bookmark-option` | `job-bookmark-enable` | 증분 읽기 활성화 |
| `--enable-s3-parquet-optimized-committer` | `1` | 원자적 커밋 |
| `--codec` | `zstd` | Parquet 압축(고정) |
| `--target_file_mb` | `256` | 파일 타깃 크기(MB) |

## 입력(Contract)

| 항목 | 값 |
|---|---|
| 경로 | `s3://<raw-bucket>/<domain>/<table>/ingestion_date=<YYYY-MM-DD>/` |
| 파티션 키 | `ingestion_date` (UTC) |
| 스키마(예) | `symbol:string, ts_utc:timestamp, price:decimal(18,6), exchange:string` |
| 최소 데이터 | 파티션당 ≥ 1 파일 |

## 출력(Contract)

| 항목 | 값 |
|---|---|
| 경로 | `s3://<curated-bucket>/<domain>/<table>/ds=<YYYY-MM-DD>/` |
| 파티션 키 | `ds` (UTC) |
| 포맷 | Parquet + ZSTD |
| 파일 크기 | 128–512MB 권장(평균) |
| 카탈로그 | Glue Database: `<domain>_db`, Table: `<table>` |

## 데이터 품질(DQ)

| 구분 | 규칙 예시 | 실패 시 동작 |
|---|---|---|
| 치명 | 스키마/타입 일치, PK not null, 음수 금지, 값 범위 | 잡 실패(커밋 차단) |
| 경고 | null 비율 < 5%, 범주 화이트리스트 일부 위반 | 통과 + 로그/메트릭 집계 |

## 성능/자원

| 항목 | 권장 |
|---|---|
| 오토스케일 | 비활성화(DPU=2 고정) |
| 파티션 스캔 | 파티션 프루닝(`ds` 범위) + 북마크 |
| 셔플/조인 | 소규모 룩업은 브로드캐스트, 불필요한 repartition 지양 |
| 파일 병합 | `coalesce` 또는 `maxRecordsPerFile`로 타깃 파일 크기 맞춤 |

## 관측성

| 항목 | 값 |
|---|---|
| 로그 | CloudWatch Logs(성공/실패 요약) |
| 메트릭 | rows_out, bytes_out, file_count(로그 기반 집계) |
| 알람 | AWS/States `ExecutionsFailed`(StateMachine), 필요 시 Metric Filter |

## 보안/IAM

| 리소스 | 권한(최소) |
|---|---|
| S3 Raw | `List/Get` on `arn:aws:s3:::<raw-bucket>/<domain>/<table>/*` |
| S3 Curated | `Put/List` on `arn:aws:s3:::<curated-bucket>/<domain>/<table>/*` |
| KMS | 관련 키 `Decrypt/Encrypt`(로그/버킷) |
| Logs | `CreateLogStream/PutLogEvents` (해당 LogGroup) |

## 태그(예시)

| Key | Value |
|---|---|
| `product` | `<domain>` |
| `dataset` | `<table>` |
| `pipeline` | `transform` |
| `environment` | `dev|stg|prod` |
