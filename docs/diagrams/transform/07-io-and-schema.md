# IO & Schema (Mermaid)

```mermaid
flowchart LR
  RAW[("RAW S3<br/>/<domain>/<table>/interval=…/data_source=…/year=YYYY/month=MM/day=DD/")] --> READ["Schema-on-read"]
  READ --> XFORM["Transform"]
  XFORM --> CUR[("Curated S3<br/>.../layer=<name>/ (Parquet + ZSTD)")]
  CUR --> ATH[("Athena / Glue Catalog<br/>domain_table views")]

  classDef store fill:#e8f3ff,stroke:#1f77b4,color:#1f77b4;
  class RAW,CUR,ATH store;
```

```mermaid
classDiagram
  class InputRecord {
    symbol
    timestamp (UTC)
    open/high/low/close?
    adjusted_close?
    volume?
    interval
    data_source
  }

  class OutputRecord {
    symbol
    timestamp (UTC)
    open/high/low/close
    adjusted_close
    volume
    ds
    layer
  }

  InputRecord <--> OutputRecord : validate & enrich
```

비고

- RAW 파티션 키: `interval`, `data_source`, `year`, `month`, `day`.
- Curated 파티션 키: `year`, `month`, `day`, `layer` (`ds` 컬럼 포함).
- 스키마 지문은 `s3://<artifacts>/<domain>/<table>/_schema/latest.json`에 기록되어 Preflight/Schema Decider에서 활용됩니다.
```
