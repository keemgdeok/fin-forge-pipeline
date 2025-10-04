# Glue Job 명세

| 항목 | 컴팩션 잡 | 변환 잡 |
|------|-----------|---------|
| 코드 경로 | `src/glue/jobs/raw_to_parquet_compaction.py` | `src/glue/jobs/daily_prices_data_etl.py` |
| Glue 버전 | 5.0 | 5.0 |
| 런타임 | Python (PySpark) | Python (PySpark) |
| 워커 타입/수 | `config.compaction_worker_type`, `config.compaction_number_workers` | G.1X, DPU 고정 2 |
| 타임아웃 | `config.compaction_timeout_minutes` 분 | 30분 |
| 재시도 | 1회 | 1회 (`ConcurrentRunsExceeded` 전용) |

## 컴팩션 Glue Job

| 항목 | 값 |
|------|-----|
| 잡 이름 | `<environment>-<table>-compaction` |
| 입력 경로 | `raw/.../interval=<interval>/data_source=<source>/year=<YYYY>/month=<MM>/day=<DD>/` |
| 출력 경로 | `curated/.../layer=<layer>` (`config.compaction_output_subdir`, 기본 `compacted`) |
| 출력 포맷 | Parquet + ZSTD |
| 파라미터 | `--ds`, `--interval`, `--data_source`, `--raw_bucket`, `--raw_prefix`, `--compacted_bucket`, `--layer`, `--codec`, `--target_file_mb` |
| 조건부 동작 | RAW 객체가 없거나 레코드 수가 0이면 출력 없이 종료 |

## Transform Glue Job

| 항목 | 값 |
|------|-----|
| 잡 이름 | `<environment>-daily-prices-data-etl` |
| 입력 경로 우선순위 | 1) `curated/.../layer=<compacted>` 2) RAW 경로(컴팩션 미존재 시) |
| 출력 경로 | `curated/.../layer=<curated_layer>` (기본 `adjusted`) |
| 파티션 키 | `year`, `month`, `day`, `layer` (+데이터 컬럼 `ds`) |
| 파라미터 | `--ds`, `--raw_bucket`, `--raw_prefix`, `--compacted_bucket`, `--compacted_layer`, `--curated_bucket`, `--curated_layer`, `--interval`, `--data_source`, `--codec`, `--target_file_mb`, `--schema_fingerprint_s3_uri` |
| DQ 실패 | `RuntimeError("DQ_FAILED: ...")` 발생 → Step Functions Catch |

## 데이터 품질 규칙

| 구분 | 예시 규칙 | 처리 |
|------|-----------|------|
| 치명 | null 심볼, 음수 가격·거래량, 레코드 0건 | RuntimeError → 잡 실패 |
| 경고 | 낮은 레코드 수, 중복 키, 타입 이상 | 경고 로그 후 계속 |
| 격리 | `layer=quarantine/year=.../` 경로에 Write 후 실패 | Preflight가 재실행 시 멱등성 체크 |

## 성능 및 리소스

| 항목 | 권장 |
|------|------|
| 파일 크기 | 최종 Parquet 128–512MB (compaction `--target_file_mb=256`) |
| 파티션 스캔 | `year/month/day` 조건으로 프루닝 |
| 셔플/조인 | 소규모 룩업은 브로드캐스트, 불필요한 repartition 지양 |
| Spark 설정 | compaction: `spark.sql.shuffle.partitions=1`; transform: `coalesce(1)` |

## 관측 및 운영

| 항목 | 설명 |
|------|------|
| 스키마 지문 | `s3://<artifacts>/<domain>/<table>/_schema/latest.json` 업데이트 → Schema Decider에 활용 |
| CloudWatch 로그 | 성공/실패 및 DQ 통계 출력 |
| IAM 최소 권한 | S3 RAW `List/Get`, S3 Curated `Put/List`, 필요한 KMS 권한 |
| 태깅 예시 | `product=<domain>`, `dataset=<table>`, `pipeline=transform`, `environment=<env>` |

---

| 관련 문서 | 경로 |
|----------|------|
| 상태 머신 계약 | `docs/specs/transform/state-machine-contract.md` |
| Preflight 입력 | `docs/specs/extract/orchestrator-contract.md` |
