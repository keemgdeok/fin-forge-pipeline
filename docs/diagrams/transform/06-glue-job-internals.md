# Glue Job Internals (Mermaid)

```mermaid
flowchart TD
  subgraph Init
    A1["Load args"] --> A2["Tune Spark config<br/>(zstd, partition size, shuffle=1)"]
  end

  subgraph Read
    R1["Resolve compacted path"] --> R2{Exists?}
    R2 -->|yes| R3["Read compacted Parquet"]
    R2 -->|no| R4["Read RAW partition"]
    R4 --> R3
  end

  subgraph Transform
    T1["Normalize & cast"] --> T2["Apply business logic"]
  end

  subgraph DataQuality
    DQ1["Evaluate DQ rules"] --> DQ2{Critical issues?}
    DQ2 -->|yes| DQF["Raise RuntimeError"]
    DQ2 -->|no| DQP["Continue"]
  end

  subgraph Write
    W1["Coalesce(1)"] --> W2["Write curated Parquet"]
    W2 --> FP["Update schema fingerprint"]
  end

  A2 --> R1 --> R2 --> T1 --> T2 --> DQ1 --> DQP --> W1 --> W2
```

비고

- Glue 5.0 환경에서 실행되며 Spark 설정은 잡 코드에서 직접 제어합니다.
- DQ 위반 시 `RuntimeError`를 던져 Step Functions에서 캐치합니다.
- 출력 파티션 키: `interval`, `data_source`, `year`, `month`, `day`, `layer` (`ds` 컬럼 포함).
- 스키마 지문은 아티팩트 버킷에 `latest.json`/`previous.json`으로 유지됩니다.
```
