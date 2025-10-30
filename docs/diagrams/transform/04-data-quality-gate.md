```mermaid
flowchart TD
  subgraph Glue_Transform
    R["Read compacted layer<br/>또는 RAW fallback"] --> S["Schema/Type checks"]
    S --> C["Business constraints<br/>(PK not null, ranges, enums)"]
    C --> D{Critical violations > 0?}
    D -->|Yes| Q["Quarantine write<br/>(.../layer=quarantine/year=YYYY/.../day=DD/)"]
    Q --> F[["Fail job (no commit)"]]
    D -->|No| W{Warnings > 0?}
    W -->|Yes| AN["Log warnings (stdout)<br/>데이터 변경 없음"]
    W -->|No| T["Apply domain transforms"]
    AN --> T
    T --> CO["Coalesce(1) → single Parquet file"]
    CO --> CUR[("S3 Curated<br/>interval=…/data_source=…/year=YYYY/month=MM/day=DD/layer=adjusted")]
  end

  classDef store fill:#e8f3ff,stroke:#1f77b4,color:#1f77b4;
  class CUR store;
```
