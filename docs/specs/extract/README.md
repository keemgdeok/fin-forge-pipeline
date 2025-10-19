# Extract Pipeline — Data Interfaces

| Step                   | Producer            | Consumer                | Contract                                                        |
| ---------------------- | ------------------- | ----------------------- | --------------------------------------------------------------- |
| Trigger → Orchestrator | EventBridge / CLI   | Orchestrator Lambda     | `docs/specs/extract/event-driven-triggers.md` (이벤트 페이로드) |
| Orchestrator → Worker  | Orchestrator Lambda | `{env}-ingestion-queue` | `docs/specs/extract/orchestrator-contract.md` (SQS 메시지)      |
| Worker → RAW S3        | Worker Lambda       | RAW 버킷                | `docs/specs/extract/raw-data-schema.md` (객체 스키마)           |
| Worker → DynamoDB      | Worker Lambda       | 배치 트래커 테이블      | `docs/specs/extract/worker-contract.md` (배치 항목)             |
| Worker → Manifest      | Worker Lambda       | Step Functions Runner   | `docs/specs/extract/worker-contract.md` (매니페스트 JSON)       |
