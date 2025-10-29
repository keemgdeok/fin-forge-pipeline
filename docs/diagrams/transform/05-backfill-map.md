```mermaid
flowchart TD
  A["Backfill 입력<br/>manifest_keys[...]"] --> M["Map 상태"]

  subgraph ItemProcessor
    M --> P["Preflight Lambda"]
    P --> D{proceed?}
    D -->|skip| SKIP(["Return false"])
    D -->|error| FAIL(["Fail"])
    D -->|run| COMP["Glue Compaction"]
    subgraph GlueJobs
      COMP --> GUARD["Compaction guard"]
      GUARD -->|no data| SKIP
      GUARD -->|process| ETL["Curated ETL"]
      ETL --> IND["Indicators ETL"]
    end
    IND --> DECIDE["Schema decider"]
    DECIDE -->|true| TRUE["Return true"]
    DECIDE -->|false| SKIP
  end

  subgraph Result
    SKIP --> Z["manifest_results"]
    TRUE --> Z
    Z --> AGG["ArrayContains == true?"]
    AGG -->|true| CRAWLER["Start crawler once"]
    AGG -->|false| DONE["Succeed"]
    CRAWLER --> DONE
  end

  classDef skip fill:#eef6ff,stroke:#5080c1,color:#2d5a8d;
  class SKIP,Z skip;
```
