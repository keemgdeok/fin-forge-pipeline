# IO & Schema (Mermaid)

```mermaid
flowchart LR
  RAW[("S3 Raw<br/>/<domain>/<table>/ingestion_date=YYYY-MM-DD/")] --> READ["Read with schema on read"]
  READ --> XFORM["Transform"]
  XFORM --> CUR[("S3 Curated<br/>/<domain>/<table>/ds=YYYY-MM-DD/ (Parquet, ZSTD)")]
  CUR --> ATH[("Athena Table<br/><domain>_<table>")]

  classDef store fill:#e8f3ff,stroke:#1f77b4,color:#1f77b4;
  class RAW,CUR,ATH store;
```

```mermaid
classDiagram
  class InputRecord {
    +string symbol
    +datetime ts_utc
    +decimal price
    +string exchange
    +string ingestion_date  // partition key (YYYY-MM-DD)
  }

  class OutputRecord {
    +string symbol
    +datetime ts_utc
    +decimal price
    +string exchange
    +date ds  // partition key (YYYY-MM-DD)
  }

  InputRecord <--> OutputRecord : mapping & validation
```

비고

- 입력 파티션 키: `ingestion_date=YYYY-MM-DD` (UTC)
- 출력 파티션 키: `ds=YYYY-MM-DD` (UTC)
- 파일 포맷: Parquet + ZSTD
- 테이블 노출: Glue Catalog/Athena 테이블 `<domain>_<table>`
- 카탈로그 갱신: 스키마 변경 감지 시에만 크롤러 실행(기본은 스킵)
