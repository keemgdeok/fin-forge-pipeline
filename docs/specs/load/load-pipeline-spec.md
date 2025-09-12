# Load Pipeline Specification (Pull Architecture)

본 문서는 Load 파이프라인의 Pull 방식(On‑premise ClickHouse → AWS S3) 아키텍처를 정의합니다. Curated S3 Object Created 이벤트를 기반으로 SQS에 작업을 게시하고, 온프레미스의 ClickHouse 로더 에이전트가 메시지를 폴링하여 S3에서 데이터를 ‘가져와’ ClickHouse에 적재합니다. DLQ로 실패를 격리합니다.

## Pipeline Architecture

### Event Flow (S3 → SQS → On‑prem Loader)

| Stage | Component | Input | Output | Target Latency |
|-------|-----------|-------|--------|----------------|
| **Trigger** | S3 Event Notification | Object Created | EventBridge Event | < 100ms |
| **Filter** | EventBridge Rule | S3 Event | Filtered Event | < 200ms |
| **Transform** | Input Transformer | Event JSON | SQS Message | < 100ms |
| **Queue** | SQS Main Queue | SQS Message | Loader Long Poll | < 300ms |
| **Process** | On‑prem CH Loader | SQS Message | ClickHouse INSERT (via `s3()` in SELECT) | < 30s |
| **Cleanup** | SQS Delete | Loader ACK | Message Deletion | < 100ms |

설명:
- On‑prem CH Loader(에이전트)는 SQS Long Poll(최대 20초)로 메시지를 가져오고, 각 메시지의 S3 객체를 ClickHouse `s3` 테이블 함수를 사용해 직접 읽어 `INSERT INTO <table> SELECT * FROM s3(...)` 형태로 적재합니다.
- 부분 실패는 메시지 단위로 처리합니다. 성공한 메시지는 즉시 Delete, 실패한 메시지는 가시성 타임아웃 경과 후 재전달되며, `maxReceiveCount` 초과 시 DLQ로 이동합니다.

### Pull 방식 개요 및 장단점

장점:
- **강력한 보안**: 온프레미스에서 AWS로의 아웃바운드만 필요(인바운드 포트 개방 불필요).
- **단순 네트워크 구성**: 퍼블릭 S3/SQS 엔드포인트로 접근만 허용하면 동작.
- **성능/안정성**: ClickHouse의 S3 병렬 읽기 최적화로 대용량 처리에 유리.
- **운영 단순성**: AWS 쪽은 IAM과 큐/버킷 정책 관리, 적재 로직은 CH가 담당.

유의사항(Trade‑offs):
- **크리덴셜 관리**: 온프레미스에 IAM 사용자/역할 자격증명 안전 저장 필요(회전/감사 포함).
- **가시성/모니터링**: Lambda 내장 지표 대신 에이전트/CH 지표를 별도 수집/전송 필요.
- **부분 실패 제어**: Lambda의 `batchItemFailures` 대신 메시지 단위 ACK/가시성 제어로 구현.
- **접근제어 세분화**: S3 프리픽스, SQS 큐 단위 최소권한을 엄격히 적용.

### Domain Configuration

| Domain | S3 Prefix | Table Pattern | Batch Size | Priority |
|--------|-----------|---------------|------------|----------|
| **market** | `market/` | `market.{table_name}` | 5-10 | High |
| **customer** | `customer/` | `customer.{table_name}` | 3-7 | Medium |
| **product** | `product/` | `product.{table_name}` | 1-5 | Low |
| **analytics** | `analytics/` | `analytics.{table_name}` | 1-3 | Low |

## Event Triggers

### S3 Event Pattern

| Field | Pattern | Value | Description |
|-------|---------|-------|-------------|
| **source** | equals | `"aws.s3"` | S3 service events only |
| **detail-type** | equals | `"Object Created"` | Creation events only |
| **bucket** | equals | `"<curated-bucket>"` | Curated data bucket |
| **key** | prefix | `"<domain>/"` | Domain-based filtering |
| **key** | suffix | `".parquet"` | Parquet files only |
| **size** | numeric > | `1024` | Non-empty files |

### Message Transformation (EventBridge → SQS)

| Output Field | Input Path | Data Type | Required | Example |
|--------------|------------|-----------|----------|---------|
| **bucket** | `$.detail.bucket.name` | string | ✅ | `data-pipeline-curated-dev` |
| **key** | `$.detail.object.key` | string | ✅ | `market/prices/ds=2025-09-10/part-001.parquet` |
| **domain** | Parsed from key | string | ✅ | `market` |
| **table_name** | Parsed from key | string | ✅ | `prices` |
| **partition** | Parsed from key | string | ✅ | `ds=2025-09-10` |
| **file_size** | `$.detail.object.size` | integer | ❌ | `1048576` |
| **correlation_id** | Generated UUID | string | ✅ | `550e8400-e29b-41d4-a716-446655440000` |

## Error Handling

### Error Codes

| Code | Description | Retry | DLQ | Recovery |
|------|-------------|:-----:|:---:|----------|
| **E001** | Invalid event schema | ❌ | ✅ | Manual review |
| **E002** | Unsupported file format | ❌ | ✅ | File reprocessing |
| **E003** | Parse failure | ❌ | ✅ | Data correction |
| **E004** | Connection timeout | ✅ | After 3 attempts | Auto-retry |
| **E005** | Database unavailable | ✅ | After 3 attempts | Service recovery |
| **E006** | Memory exhaustion | ✅ | After 2 attempts | Resource scaling |

### Retry Policy

| Error Type | Max Attempts | Backoff | Total Time | DLQ Action |
|------------|:------------:|---------|:----------:|------------|
| **Connection** | 3 | 2s, 4s, 8s | ~14s | After exhaustion |
| **Network** | 3 | 2s, 4s, 8s | ~14s | After exhaustion |
| **Memory** | 2 | 5s, 10s | ~15s | After exhaustion |
| **Schema** | 1 | None | Immediate | Immediate |
| **Permission** | 1 | None | Immediate | Immediate |

## Monitoring

### Key Metrics

| Metric | Description | Target | Alert Threshold |
|--------|-------------|:------:|:---------------:|
| **ProcessingLatency** | End-to-end latency | < 5 min | > 10 min |
| **SuccessRate** | Successful processing rate | > 99% | < 95% |
| **ErrorRate** | Failed processing rate | < 1% | > 5% |
| **QueueDepth** | SQS message count | < 50 | > 100 |
| **DLQCount** | Dead letter queue messages | 0 | > 0 |

### CloudWatch Alarms

| Alarm | Metric | Condition | Evaluation | Priority |
|-------|--------|-----------|------------|:--------:|
| **LoadLatencyHigh** | ProcessingLatency | > 10 min | 2 of 3 (5min) | P1 |
| **LoadErrorRateHigh** | ErrorRate | > 5% | 3 of 3 (5min) | P0 |
| **LoadQueueDepthHigh** | QueueDepth | > 100 | 2 of 3 (5min) | P1 |
| **LoadDLQMessages** | DLQCount | > 0 | 1 of 1 (1min) | P0 |

## Performance

### Scaling Configuration (On‑prem Loader centric)

| Component | Setting | Value | Scaling Trigger |
|-----------|---------|-------|----------------|
| **Loader Concurrency** | Worker threads | 5–20 | Queue depth > 20 |
| **SQS Batch Size** | `MaxNumberOfMessages` | 5–10 | File size/domain priority |
| **Visibility Timeout** | Duration | 1800s | ≥ 6× per‑message timeout |
| **Per‑message Timeout** | Loader | 300s | 5 minutes max |

### Capacity Planning

| Domain | Peak Load | Avg File Size | Batch Size | Loader Memory |
|--------|:---------:|:-------------:|:----------:|:-------------:|
| **market** | 1000 msg/hr | 5MB | 5 | 768MB |
| **customer** | 500 msg/hr | 10MB | 3 | 1024MB |
| **product** | 200 msg/hr | 2MB | 10 | 512MB |
| **analytics** | 100 msg/hr | 50MB | 1 | 1024MB |

## Security

### IAM Requirements (Least privilege)

| Component | Required Permissions | Resource Pattern |
|-----------|---------------------|------------------|
| **EventBridge Rule** | `events:PutEvents` | `arn:aws:events:<region>:<account>:event-bus/default` |
| **SQS Target** | `sqs:SendMessage` | `arn:aws:sqs:<region>:<account>:<env>-<domain>-load-queue` |
| **On‑prem Loader (IAM User/Role)** | `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:ChangeMessageVisibility` | 해당 SQS Queue ARN |
| **On‑prem Loader (S3)** | `s3:GetObject` | `arn:aws:s3:::<curated-bucket>/<domain>/*` |

네트워킹:
- 온프레미스 → AWS 아웃바운드만 필요(S3/SQS 퍼블릭 엔드포인트 접근). 인바운드 포트 개방 불필요.
- 가능하면 VPC 엔드포인트/프라이빗 링크 대신, 온프레미스 egress 정책과 퍼블릭 AWS 엔드포인트 허용 리스트를 관리합니다.

### Data Protection

| Security Layer | Implementation | Configuration |
|----------------|----------------|---------------|
| **Encryption in Transit** | TLS 1.2+ | All API calls |
| **Encryption at Rest** | SSE-S3/KMS | S3 objects, SQS messages |
| **Network Isolation** | On‑prem outbound only | Allowlist S3/SQS endpoints |
| **Access Control** | IAM User/Role | Least privilege principle |

## TDD Test Coverage

### Core Test Scenarios (15 essential tests)

| Test Category | Test Cases | Coverage |
|---------------|:----------:|----------|
| **Event Processing** | 5 | Pattern matching, transformation, validation |
| **Error Handling** | 4 | Retry logic, DLQ routing, timeout handling |
| **Integration Flow** | 3 | S3→EventBridge→SQS→On‑prem Loader end‑to‑end |
| **Performance** | 2 | Load handling, resource management |
| **Security** | 1 | IAM permissions, data encryption |

### Test Implementation

| Test Type | Scope | Execution | Environment |
|-----------|-------|-----------|-------------|
| **Unit** | Component logic | Automated | Local/CI |
| **Integration** | Cross-component | Automated | Dev/Staging |
| **Load** | Performance limits | Manual | Staging only |
| **Security** | Access controls | Automated | All environments |
