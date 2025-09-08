```mermaid
sequenceDiagram
  autonumber
  participant SF as Step Functions
  participant PRE as Preflight Lambda
  participant GLUE as Glue ETL Job
  participant RAW as S3 Raw Bucket
  participant CUR as S3 Curated Bucket
  participant CR as Glue Crawler
  participant CAT as Glue Data Catalog
  participant EV as EventBridge

  SF->>PRE: Invoke (domain, table, date)
  PRE-->>SF: Glue args + idempotency key
  alt Preflight 실패
    SF-->>SF: Fail
    SF-->>SF: Fail
  else Preflight 통과
    SF->>GLUE: StartJobRun(args)
    GLUE->>RAW: Read partitions
    GLUE->>GLUE: Transform + validate
    GLUE->>CUR: Write Parquet (partitioned)
    GLUE-->>SF: Success(runId, stats)
    SF->>PRE: Schema drift check
    PRE-->>SF: changed? (true/false)
    alt Schema changed
      SF->>CR: StartCrawler
      CR->>CUR: Scan new paths
      CR-->>CAT: Update table/partitions
    else No change
      SF-->>SF: Skip crawler
    end
    SF->>EV: PutEvent(DataReady)
    SF-->>SF: Succeed
  end

  Note over GLUE: 품질 실패/격리 경로는 04-data-quality-gate 참조
```
