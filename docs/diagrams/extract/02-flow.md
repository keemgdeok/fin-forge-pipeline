```mermaid
flowchart TD
  A["EventBridge Scheduled Event<br/>env-configured payload"] --> B["Orchestrator Lambda"]
  B --> C["Resolve symbols<br/>(SSM → S3 → event payload → default)"]
  C --> D["Chunk symbols (size N)"]
  D --> E["Init/refresh DynamoDB batch tracker\n(expected_chunks, status='processing')"]
  E --> F["SendMessageBatch to ingestion SQS"]
  F --> G["SQS → Ingestion Worker Lambda"]
  G --> H["Fetch prices from provider"]
  H --> I["Group records by symbol & ds"]
  I --> J{RAW object exists?}
  J -->|Yes| K["Skip write (idempotent)"]
  J -->|No| L["Serialize and PutObject\ninterval/data_source/year/month/day"]
  L --> M["Record written key"]
  K --> M
  M --> N["Update DynamoDB tracker\n(ADD processed_chunks, append partition summary)"]
  N --> O{All chunks processed?}
  O -->|No| P["Return partial success"]
  O -->|Yes| Q["Emit partition manifests\n(_batch.manifest.json)"]
  Q --> R["Runner scripts 수집\n(`scripts/validate_pipeline.py` 등)"]
  R --> S["Start Step Functions execution\n(manifest_keys 입력)"]

```
