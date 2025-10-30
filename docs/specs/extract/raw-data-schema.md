# Raw Data Schema — Data Contract

| 항목      | 값                                                                        |
| --------- | ------------------------------------------------------------------------- |
| 목적      | Ingestion Worker가 생성하는 RAW S3 객체의 경로 및 레코드 스키마 정의      |
| 코드 기준 | `src/lambda/layers/data/market/python/market_shared/ingestion/service.py` |
| 소비자    | Transform/Compaction Glue Jobs, Preflight Lambda                          |

### S3 경로 구조

| 수준 | 세그먼트                                  | 예시                        | 설명               |
| ---- | ----------------------------------------- | --------------------------- | ------------------ |
| 1    | `{domain}`                                | `market`                    | 비즈니스 도메인    |
| 2    | `{table_name}`                            | `prices`                    | 데이터셋           |
| 3    | `interval={interval}`                     | `interval=1d`               | 수집 주기          |
| 4    | `data_source={data_source}`               | `data_source=yahoo_finance` | 공급자 구분        |
| 5    | `year={YYYY}`                             | `year=2025`                 | 연도               |
| 6    | `month={MM}`                              | `month=09`                  | 월                 |
| 7    | `day={DD}`                                | `day=07`                    | 일                 |
| 파일 | `{symbol}.{ext}` 또는 `{symbol}.{ext}.gz` | `AAPL.json`                 | 심볼별 일일 데이터 |

### Price Record 스키마 (JSON Lines / CSV 행)

| 필드             | 타입    | 필수 | 제약                                  |
| ---------------- | ------- | :--: | ------------------------------------- |
| `symbol`         | string  |  ✅  | 1–20자, 대문자/숫자/`.`/`-`           |
| `timestamp`      | string  |  ✅  | ISO 8601 UTC (`YYYY-MM-DDTHH:MM:SSZ`) |
| `open`           | number  |  ❌  | ≥ 0                                   |
| `high`           | number  |  ❌  | ≥ 0                                   |
| `low`            | number  |  ❌  | ≥ 0                                   |
| `close`          | number  |  ❌  | ≥ 0                                   |
| `adjusted_close` | number  |  ❌  | ≥ 0                                   |
| `volume`         | integer |  ❌  | ≥ 0                                   |

### 지원 파일 형식

| 포맷       | 확장자               | 직렬화            | 비고                        |
| ---------- | -------------------- | ----------------- | --------------------------- |
| JSON Lines | `.json` / `.json.gz` | 줄 단위 JSON 객체 | 기본 저장 형식              |
| CSV        | `.csv` / `.csv.gz`   | 헤더 포함 CSV     | 필요 시 `file_format="csv"` |

각 객체는 하나의 심볼/하루 데이터를 포함하며, 동일 키가 존재하면 이후 업로드는 SKIP
