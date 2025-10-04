# Glue Job Internals (Mermaid)

```mermaid
flowchart TD
  subgraph Init
    A1["Load Glue args"] --> A2["Spark config 설정\n- parquet.codec = zstd\n- files.maxPartitionBytes<br/>- shuffle.partitions = 1"]
  end

  subgraph Read
    R1["Resolve compacted path\ncurated/.../layer=<compacted>"] --> R2{Available?}
    R2 -->|Yes| R3["Read Parquet (compacted)"]
    R2 -->|No| R4["Fallback to Raw path\nraw/.../interval=…/year/month/day/"]
    R4 --> R3
  end

  subgraph Transform
    T1["Type casting & normalization"] --> T2["Business transforms<br/>(price adjustments, enrich)"]
  end

  subgraph DataQuality
    DQ1["Evaluate rules\n- null symbol\n- 음수 가격/볼륨\n- 레코드 카운트"] --> DQ2{Violations?}
    DQ2 -->|Yes| DQF["Raise RuntimeError\n→ Step Functions Catch"]
    DQ2 -->|No| DQP["Proceed"]
  end

  subgraph Write
    W1["coalesce(1)"] --> W2["Write Parquet\ninterval/.../layer=<curated>"]
  end

  A2 --> R1 --> R2 --> T1 --> T2 --> DQ1 --> DQP --> W1 --> W2

  classDef note fill:#eef7ff,stroke:#3d7ea6,color:#244a5b;
```

비고

- 컴팩션 잡과 변환 잡 모두 Glue 5.0 환경을 사용하며, Spark 설정은 코드(`raw_to_parquet_compaction.py`, `daily_prices_data_etl.py`)에서 직접 제어합니다.
- 데이터 품질 실패 시 `RuntimeError("DQ_FAILED: …")`를 발생시켜 Step Functions Catch 체인으로 전파합니다.
- 출력 파티션: `interval`, `data_source`, `year`, `month`, `day`, `layer`. 데이터 내부에는 `ds` 컬럼이 함께 저장됩니다.
- Schema fingerprint는 변환 잡 종료 시 아티팩트 버킷에 `latest.json`/`previous.json` 형태로 기록되어 Crawler 실행 여부 판단에 활용됩니다.
```
