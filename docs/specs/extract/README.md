# Extract Pipeline — Data Interfaces

| Step                   | Producer            | Consumer                | Contract                                                                   |
| ---------------------- | ------------------- | ----------------------- | -------------------------------------------------------------------------- |
| Trigger → Orchestrator | EventBridge / CLI   | Orchestrator Lambda     | `docs/specs/extract/event-driven-triggers.md` (이벤트 페이로드, 단일 소스) |
| Orchestrator → Worker  | Orchestrator Lambda | `{env}-ingestion-queue` | `docs/specs/extract/sqs-integration-spec.md` (SQS 메시지 본문/속성)        |
| Worker → RAW S3        | Worker Lambda       | RAW Bucket              | `docs/specs/extract/raw-data-schema.md` (객체 스키마)                      |
| Worker → DynamoDB      | Worker Lambda       | Batch Tracker Table     | `docs/specs/extract/worker-contract.md` (배치 항목)                        |
| Worker → Manifest      | Worker Lambda       | Step Functions Runner   | `docs/specs/extract/worker-contract.md` (매니페스트 JSON)                  |
