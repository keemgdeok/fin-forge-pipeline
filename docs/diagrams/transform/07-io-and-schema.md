# IO & Schema (Mermaid)

```mermaid
flowchart LR
  RAW[("S3 Raw<br/>/<domain>/<table>/interval=…/data_source=…/year=YYYY/month=MM/day=DD/")] --> READ["Read with schema-on-read"]
  READ --> XFORM["Transform"]
  XFORM --> CUR[("S3 Curated<br/>/<domain>/<table>/interval=…/data_source=…/year=YYYY/month=MM/day=DD/layer=<name>/ (Parquet, ZSTD)")]
  CUR --> ATH[("Athena / Glue Catalog<br/>domain_table views")]

  classDef store fill:#e8f3ff,stroke:#1f77b4,color:#1f77b4;
  class RAW,CUR,ATH store;
```

```mermaid
classDiagram
  class InputRecord {
    +string symbol
    +datetime ts_utc
    +float? open/high/low/close
    +float? adjusted_close
    +float? volume
    +string interval
    +string data_source
    +date utc_day // year/month/day 파티션에 매핑
  }

  class OutputRecord {
    +string symbol
    +datetime ts_utc
    +float open/high/low/close
    +float adjusted_close
    +float volume
    +date ds
    +string layer // adjusted, quarantine 등
  }

  InputRecord <--> OutputRecord : transform + quality checks
```

비고

- RAW 파티션 키: `interval`, `data_source`, `year`, `month`, `day`.
- Curated 파티션 키: `year`, `month`, `day`, `layer` (`adjusted`, `compacted`, `quarantine` 등). 데이터에는 `ds` 컬럼이 포함됩니다.
- 파일 포맷: Parquet + ZSTD, Spark `coalesce(1)`로 일일당 1개 파일 기본.
- 스키마 지문은 `s3://<artifacts>/<domain>/<table>/_schema/latest.json`에 기록되며 Preflight/Schema Decider에서 활용합니다.
- Athena/Glue 노출은 IaC 또는 crawler 정책(스키마 변경 시)에 따라 관리합니다.
```
