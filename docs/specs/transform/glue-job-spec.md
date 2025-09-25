# Glue Job 명세 (표 형식)

다음 명세는 `<domain>/<table>` 파이프라인의 컴팩션 및 변환 잡 계약을 문서화합니다. 값은 환경/도메인별로 치환하세요.

## 컴팩션 Glue Job (Raw → Parquet)

### 기본 정보

| 항목 | 값 |
|---|---|
| 잡 이름 | `<environment>-<table>-compaction` |
| Glue 버전 | 5.0 |
| 언어 | Python (PySpark) |
| 워커 타입 | `config.compaction_worker_type` (예: G.1X) |
| 워커 수 | `config.compaction_number_workers` (예: 2) |
| 타임아웃(분) | `config.compaction_timeout_minutes` (예: 15) |
| 재시도 횟수 | 1 |

### 파라미터(Arguments)

| 키 | 예시 | 설명 |
|---|---|---|
| `--ds` | `2025-09-07` | 처리 대상 파티션(UTC) |
| `--interval` | `1d` | Raw 파티션 interval |
| `--data_source` | `yahoo_finance` | Raw 파티션 data_source |
| `--raw_bucket` | `raw-bucket` | 입력 버킷 |
| `--raw_prefix` | `<domain>/<table>/interval=<interval>/data_source=<source>/` | 입력 프리픽스 |
| `--compacted_bucket` | `curated-bucket` | 출력 버킷 (curated) |
| `--compacted_prefix` | `<domain>/<table>/<subdir>` | 출력 프리픽스 (`config.compaction_output_subdir`, 예: `compacted`) |
| `--codec` | `zstd` | Parquet 압축 |
| `--target_file_mb` | `256` | 파일 타깃 크기(MB) |
| `--job-bookmark-option` | `job-bookmark-disable` | 실시간 입력은 사용하지 않음 |

### 입력/출력 계약

| 구분 | 값 |
|---|---|
| 입력 경로 | `s3://<raw-bucket>/<domain>/<table>/interval=<interval>/data_source=<source>/year=<YYYY>/month=<MM>/day=<DD>/` |
| 입력 포맷 | RAW 소스 형식(JSON/CSV/Parquet) |
| 출력 경로 | `s3://<curated-bucket>/<domain>/<table>/<subdir>/ds=<YYYY-MM-DD>/` |
| 출력 포맷 | Parquet + ZSTD |
| 비고 | Raw 객체가 없거나 레코드 수가 0이면 출력 없이 종료(Transform 단계에서 성공 처리) |

## Transform Glue Job (Parquet Transform)

### 기본 정보

| 항목 | 값 |
|---|---|
| 잡 이름 | `<domain>-<table>-transform` |
| Glue 버전 | 5.0 |
| 언어 | Python (PyGlue) |
| 워커 타입 | G.1X |
| DPU (최소/최대) | 2 / 2 (오토스케일 off, 고정) |
| 타임아웃(분) | 30 |
| 재시도 횟수 | 1 |

### 파라미터(Arguments)

| 키 | 예시 | 설명 |
|---|---|---|
| `--ds` | `2025-09-07` | 처리 대상 파티션(UTC) |
| `--raw_bucket` | `raw-bucket` | 입력 버킷 |
| `--raw_prefix` | `<domain>/<table>/interval=<interval>/data_source=<source>/` | 입력 프리픽스 |
| `--compacted_bucket` | `curated-bucket` | 컴팩션 출력 버킷 |
| `--compacted_prefix` | `<domain>/<table>/<subdir>` | 컴팩션 출력 프리픽스 |
| `--curated_bucket` | `curated-bucket` | 출력 버킷 |
| `--curated_prefix` | `<domain>/<table>/` | 출력 프리픽스 |
| `--interval` | `1d` | RAW 파티션 interval | 
| `--data_source` | `yahoo_finance` | RAW 파티션 data_source |
| `--job-bookmark-option` | `job-bookmark-enable` | 증분 읽기 활성화 |
| `--enable-s3-parquet-optimized-committer` | `1` | 원자적 커밋 |
| `--codec` | `zstd` | Parquet 압축(고정) |
| `--target_file_mb` | `256` | 파일 타깃 크기(MB) |
| `--schema_fingerprint_s3_uri` | `s3://<artifacts-bucket>/<domain>/<table>/_schema/latest.json` | 스키마 지문 산출물 저장 위치(Optional) |

### 입력(Contract)

| 항목 | 값 |
|---|---|
| 경로(우선) | `s3://<curated-bucket>/<domain>/<table>/<subdir>/ds=<YYYY-MM-DD>/` |
| 경로(보조) | `s3://<raw-bucket>/<domain>/<table>/interval=<interval>/data_source=<source>/year=<YYYY>/month=<MM>/day=<DD>/` (컴팩션 결과가 없을 때만) |
| 파티션 키 | `ds` (compacted), `interval, data_source, year, month, day` (fallback) |
| 스키마(예) | `symbol:string, ts_utc:timestamp, price:decimal(18,6), exchange:string` |
| 최소 데이터 | 컴팩션 파티션 ≥ 1 파일 (없으면 잡 스킵) |

### 출력(Contract)

| 항목 | 값 |
|---|---|
| 경로 | `s3://<curated-bucket>/<domain>/<table>/ds=<YYYY-MM-DD>/` |
| 파티션 키 | `ds` (UTC) |
| 포맷 | Parquet + ZSTD |
| 파일 크기 | 128–512MB 권장(평균) |
| 카탈로그 | Glue Database: `<domain>_db`, Table: `<table>` |

### 데이터 품질(DQ)

| 구분 | 규칙 예시 | 실패 시 동작 |
|---|---|---|
| 치명 | 스키마/타입 일치, PK not null, 음수 금지, 값 범위 | 잡 실패(커밋 차단) |
| 경고 | null 비율 < 5%, 범주 화이트리스트 일부 위반 | 통과 + 요약 집계(도구 무관) |

### 성능/자원

| 항목 | 권장 |
|---|---|
| 오토스케일 | 비활성화(DPU=2 고정) |
| 파티션 스캔 | 파티션 프루닝(`ds` 범위) + 북마크 |
| 셔플/조인 | 소규모 룩업은 브로드캐스트, 불필요한 repartition 지양 |
| 파일 병합 | `coalesce` 또는 `maxRecordsPerFile`로 타깃 파일 크기 맞춤 |

### 관측성(선택)

| 항목 | 값 |
|---|---|
| 요약 | 성공/실패 및 행/바이트/파일 수 요약(도구 무관) |
| 스키마 지문 | `<artifacts>/_schema/latest.json` 또는 `<curated>/_schema/current.json`에 `{columns, types, hash}` 기록(선택) |

### 보안/IAM

| 리소스 | 권한(최소) |
|---|---|
| S3 Raw | `List/Get` on `arn:aws:s3:::<raw-bucket>/<domain>/<table>/*` |
| S3 Curated | `Put/List` on `arn:aws:s3:::<curated-bucket>/<domain>/<table>/*` |
| KMS | 관련 키 `Decrypt/Encrypt`(버킷 등 필요한 리소스) |

### 태그(예시)

| Key | Value |
|---|---|
| `product` | `<domain>` |
| `dataset` | `<table>` |
| `pipeline` | `transform` |
| `environment` | `dev|stg|prod` |
