# Glue Job Internals (Mermaid)

```mermaid
flowchart TD
  subgraph Init
    A1["Load Glue args"] --> A2["Set Spark config<br/>- dynamic allocation<br/>- shuffle partitions<br/>- optimized committer"]
  end

  subgraph Read
    R1["Resolve input partitions<br/>raw/<domain>/<table>/ingestion_date=YYYY-MM-DD/"] --> R2["Read as DataFrame<br/>(schema on read)"]
  end

  subgraph Transform
    T1["Type casting & normalization"] --> T2["Business transforms<br/>(joins/aggregates)"]
  end

  subgraph DataQuality
    DQ1["Evaluate rules<br/>- schema/type<br/>- not null/unique<br/>- ranges/enums"] --> DQ2{Violations?}
    DQ2 -->|Yes| DQF["Fail job (no commit)<br/>log: DQ summary"]
    DQ2 -->|No| DQP["Proceed"]
  end

  subgraph Write
    W1["Coalesce to target size<br/>128–512MB"] --> W2["Write Parquet (partitioned ds)"]
    W2 --> W3["Optimized commit<br/>--enable-s3-parquet-optimized-committer=1"]
  end

  A2 --> R1 --> R2 --> T1 --> T2 --> DQ1 --> DQ2
  DQP --> W1 --> W2 --> W3

  classDef note fill:#eef7ff,stroke:#3d7ea6,color:#244a5b;
```

비고

- Spark 설정: Glue 4/5, 오토스케일 on, 셔플 파티션 수는 데이터량에 맞춰 조정.
- 데이터 품질: 치명 규칙 위반 시 커밋 차단(부분 결과 노출 방지). 경고 규칙은 통과 후 로그/메트릭 집계 가능.
- 쓰기: 임시 경로 → 최종 경로 원자적 커밋. Parquet+ZSTD, 파일 크기 타깃 128–512MB.
- 증분: Job Bookmark + 파티션 프루닝으로 스캔 최소화.

권장 Spark 튜닝(DPU=2 고정 가정)

- `spark.sql.shuffle.partitions`: 데이터량에 따라 64–128 범위로 시작
- `spark.default.parallelism`: `spark.sql.shuffle.partitions`와 동일값 권장
- 작은 룩업 조인: 브로드캐스트 조인 사용, 불필요한 `repartition` 지양
- 파일 병합: `coalesce(n)` 또는 `maxRecordsPerFile`로 평균 256MB 근접
