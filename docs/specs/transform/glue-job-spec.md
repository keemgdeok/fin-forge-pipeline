# Glue Job 명세

| 항목         | 컴팩션 잡                                                           | 변환 잡                                           | 인디케이터 잡                                     |
| ------------ | ------------------------------------------------------------------- | ------------------------------------------------- | ------------------------------------------------- |
| 코드 경로    | `src/glue/jobs/raw_to_parquet_compaction.py`                        | `src/glue/jobs/daily_prices_data_etl.py`          | `src/glue/jobs/market_indicators_etl.py`          |
| Glue 버전    | 5.0                                                                 | 5.0                                               | 5.0                                               |
| 런타임       | Python (PySpark)                                                    | Python (PySpark)                                  | Python (PySpark)                                  |
| 워커 타입/수 | `config.compaction_worker_type`, `config.compaction_number_workers` | `G.1X`, `config.glue_max_capacity` (기본 2)       | `G.1X`, `config.glue_max_capacity` (기본 2)       |
| 타임아웃     | `config.compaction_timeout_minutes` 분                              | 30분                                              | 30분                                              |
| 재시도       | 1회                                                                 | 1회 (`Glue.ConcurrentRunsExceededException` 전용) | 1회 (`Glue.ConcurrentRunsExceededException` 전용) |

### Compaction Glue Job

| 항목        | 값                                                                                                                                    |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| 잡 이름     | `<environment>-<table>-compaction`                                                                                                    |
| 입력 경로   | `raw/.../interval=<interval>/data_source=<source>/year=<YYYY>/month=<MM>/day=<DD>/`                                                   |
| 출력 경로   | `curated/.../layer=<layer>` (`config.compaction_output_subdir`, 기본 `compacted`)                                                     |
| 출력 포맷   | Parquet + ZSTD                                                                                                                        |
| 파라미터    | `--ds`, `--interval`, `--data_source`, `--raw_bucket`, `--raw_prefix`, `--compacted_bucket`, `--layer`, `--codec`, `--target_file_mb` |
| 조건부 동작 | RAW 객체가 없거나 레코드 수가 0이면 출력 없이 종료                                                                                    |

### Transform Glue Job

| 항목               | 값                                                                                                                                                                                                                    |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 잡 이름            | `<environment>-daily-prices-data-etl`                                                                                                                                                                                 |
| 입력 경로 우선순위 | 1) `curated/.../layer=<compacted>` 2) RAW 경로(컴팩션 미존재 시)                                                                                                                                                      |
| 출력 경로          | `curated/.../layer=<curated_layer>` (기본 `adjusted`)                                                                                                                                                                 |
| 파티션 키          | `year`, `month`, `day`, `layer` (+데이터 컬럼 `ds`)                                                                                                                                                                   |
| 파라미터           | `--ds`, `--raw_bucket`, `--raw_prefix`, `--compacted_bucket`, `--compacted_layer`, `--curated_bucket`, `--curated_layer`, `--interval`, `--data_source`, `--codec`, `--target_file_mb`, `--schema_fingerprint_s3_uri` |
| DQ 실패            | `RuntimeError("DQ_FAILED: ...")` 발생 → Step Functions Catch                                                                                                                                                          |

### Indicator Glue Job

| 항목          | 값                                                                                                                                                                                                                                                                    |
| ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 잡 이름       | `<environment>-market-indicators-etl`                                                                                                                                                                                                                                 |
| 입력 경로     | `curated/.../interval=<interval>/data_source=<source>/` (layer 필터 `prices_layer`, 기본 `adjusted`)                                                                                                                                                                  |
| 출력 경로     | `curated/.../layer=<output_layer>` (기본 `technical_indicator`)                                                                                                                                                                                                       |
| 파라미터      | `--ds`, `--environment`, `--prices_curated_bucket`, `--output_bucket`, `--schema_fingerprint_s3_uri`, `--codec`, `--target_file_mb`, `--lookback_days`, `--interval`, `--data_source`, `--domain`, `--table_name`, `--prices_layer`, `--output_layer`, `--uri_scheme` |
| lookback 규칙 | `--lookback_days` (기본 252) 윈도우 기간 동안 가격 데이터 요구                                                                                                                                                                                                        |
| DQ 실패       | 입력 누락, 중복 키, 지표 NaN 초과 시 `RuntimeError` 발생                                                                                                                                                                                                              |
