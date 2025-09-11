# Load Pipeline Specification

Load 파이프라인의 이벤트 기반 데이터 로딩 시스템 명세입니다. S3 Object Created 이벤트부터 ClickHouse 로딩까지의 전체 플로우를 정의합니다.

## Pipeline Architecture

### Event Flow

| Stage | Component | Input | Output | Processing Time |
|-------|-----------|-------|--------|----------------|
| **Trigger** | S3 Event Notification | Object Created | EventBridge Event | < 100ms |
| **Filter** | EventBridge Rule | S3 Event | Filtered Event | < 200ms |
| **Transform** | Input Transformer | Event JSON | SQS Message | < 100ms |
| **Queue** | SQS Main Queue | SQS Message | Lambda Trigger | < 300ms |
| **Process** | Load Lambda | SQS Batch | ClickHouse Insert | < 30s |
| **Cleanup** | SQS Response | Lambda Response | Message Deletion | < 100ms |

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

### Message Transformation

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

### Scaling Configuration

| Component | Setting | Value | Scaling Trigger |
|-----------|---------|-------|----------------|
| **Lambda Concurrency** | Reserved | 50 | Queue depth > 20 |
| **SQS Batch Size** | Messages | 5-10 | Based on file size |
| **Visibility Timeout** | Duration | 1800s | 6x Lambda timeout |
| **Processing Timeout** | Lambda | 300s | 5 minutes max |

### Capacity Planning

| Domain | Peak Load | Avg File Size | Batch Size | Lambda Memory |
|--------|:---------:|:-------------:|:----------:|:-------------:|
| **market** | 1000 msg/hr | 5MB | 5 | 768MB |
| **customer** | 500 msg/hr | 10MB | 3 | 1024MB |
| **product** | 200 msg/hr | 2MB | 10 | 512MB |
| **analytics** | 100 msg/hr | 50MB | 1 | 1024MB |

## Security

### IAM Requirements

| Component | Required Permissions | Resource Pattern |
|-----------|---------------------|------------------|
| **EventBridge Rule** | `events:PutEvents` | `arn:aws:events:<region>:<account>:event-bus/default` |
| **SQS Target** | `sqs:SendMessage` | `arn:aws:sqs:<region>:<account>:<env>-<domain>-load-queue` |
| **Lambda Function** | `sqs:ReceiveMessage`, `sqs:DeleteMessage` | SQS Queue ARN |
| **S3 Access** | `s3:GetObject` | `arn:aws:s3:::<curated-bucket>/<domain>/*` |
| **ClickHouse Access** | Network access to DW cluster | VPC Security Group |

### Data Protection

| Security Layer | Implementation | Configuration |
|----------------|----------------|---------------|
| **Encryption in Transit** | TLS 1.2+ | All API calls |
| **Encryption at Rest** | SSE-S3/KMS | S3 objects, SQS messages |
| **Network Isolation** | VPC | Lambda in private subnets |
| **Access Control** | IAM Roles | Least privilege principle |

## TDD Test Coverage

### Core Test Scenarios (15 essential tests)

| Test Category | Test Cases | Coverage |
|---------------|:----------:|----------|
| **Event Processing** | 5 | Pattern matching, transformation, validation |
| **Error Handling** | 4 | Retry logic, DLQ routing, timeout handling |
| **Integration Flow** | 3 | S3→EventBridge→SQS→Lambda end-to-end |
| **Performance** | 2 | Load handling, resource management |
| **Security** | 1 | IAM permissions, data encryption |

### Test Implementation

| Test Type | Scope | Execution | Environment |
|-----------|-------|-----------|-------------|
| **Unit** | Component logic | Automated | Local/CI |
| **Integration** | Cross-component | Automated | Dev/Staging |
| **Load** | Performance limits | Manual | Staging only |
| **Security** | Access controls | Automated | All environments |