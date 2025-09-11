# Load Component Contracts

Load 파이프라인 컴포넌트들의 I/O 계약 및 구성 명세입니다. SQS, Lambda, ClickHouse 간의 데이터 계약과 연결 설정을 정의합니다.

## SQS Message Contracts

### Message Schema

| Field | Type | Required | Format | Example |
|-------|------|:--------:|--------|---------|
| **bucket** | string | ✅ | S3 bucket name | `data-pipeline-curated-dev` |
| **key** | string | ✅ | S3 object key | `market/prices/ds=2025-09-10/part-001.parquet` |
| **domain** | string | ✅ | [a-z0-9-]{1,50} | `market` |
| **table_name** | string | ✅ | [a-z0-9_]{1,50} | `prices` |
| **partition** | string | ✅ | ds=YYYY-MM-DD | `ds=2025-09-10` |
| **file_size** | integer | ❌ | > 0 | `1048576` |
| **correlation_id** | string | ✅ | UUID v4 | `550e8400-e29b-41d4-a716-446655440000` |

### Message Attributes

| Attribute | Type | Purpose | Example |
|-----------|------|---------|---------|
| **ContentType** | String | Message format | `application/json` |
| **Domain** | String | Routing filter | `market` |
| **TableName** | String | Table filter | `prices` |
| **Priority** | Number | Processing order | `1` (High), `2` (Medium), `3` (Low) |

### Queue Configuration

| Setting | Main Queue | Dead Letter Queue | Description |
|---------|:----------:|:----------------:|-------------|
| **Name Pattern** | `<env>-<domain>-load-queue` | `<env>-<domain>-load-dlq` | Environment and domain scoped |
| **Visibility Timeout** | 1800s | 300s | 6x and 1x Lambda timeout |
| **Message Retention** | 14 days | 14 days | Maximum retention |
| **Max Receive Count** | 3 | Unlimited | Retry limit before DLQ |

## Lambda Function Contracts

### Input Contract

| Parameter | Source | Type | Validation |
|-----------|--------|------|------------|
| **Records** | SQS Event | Array[SQSRecord] | 1-10 records per batch |
| **SQSRecord.body** | Message body | JSON string | Valid JSON schema |
| **SQSRecord.messageAttributes** | SQS attributes | Map[string, any] | Optional attributes |
| **SQSRecord.receiptHandle** | SQS handle | string | Required for deletion |

### Output Contract

| Response Field | Type | Required | Purpose |
|----------------|------|:--------:|---------|
| **batchItemFailures** | Array[FailureRecord] | ✅ | Partial batch failure support |
| **FailureRecord.itemIdentifier** | string | ✅ | Message ID for retry |

### Error Response Codes

| Code | HTTP Status | Description | Retry Action | DLQ Action |
|------|:-----------:|-------------|:------------:|:----------:|
| **PARSE_ERROR** | 400 | Invalid JSON in message body | ❌ | ✅ |
| **VALIDATION_ERROR** | 400 | Schema validation failure | ❌ | ✅ |
| **FILE_NOT_FOUND** | 404 | S3 object not accessible | ✅ | After 3 attempts |
| **CONNECTION_ERROR** | 503 | Database connection failure | ✅ | After 3 attempts |
| **TIMEOUT_ERROR** | 504 | Processing timeout | ✅ | After 2 attempts |

### Environment Variables

| Variable | Format | Required | Example | Description |
|----------|--------|:--------:|---------|-------------|
| **CLICKHOUSE_HOST** | hostname:port | ✅ | `clickhouse.example.com:9000` | Database endpoint |
| **CLICKHOUSE_DATABASE** | string | ✅ | `data_warehouse` | Target database |
| **CLICKHOUSE_USER** | string | ✅ | `load_user` | Database username |
| **MAX_CONNECTIONS** | integer | ❌ | `8` | Connection pool size |
| **QUERY_TIMEOUT** | integer | ❌ | `30` | Query timeout (seconds) |
| **LOG_LEVEL** | enum | ❌ | `INFO` | Logging verbosity |

## ClickHouse Integration

### Connection Parameters

| Parameter | Type | Default | Range | Description |
|-----------|------|:-------:|-------|-------------|
| **host** | string | - | - | ClickHouse server hostname |
| **port** | integer | 9000 | 1024-65535 | Native protocol port |
| **database** | string | - | - | Target database name |
| **user** | string | - | - | Authentication username |
| **password** | string | - | - | Authentication password (from secrets) |
| **timeout** | integer | 30 | 5-300 | Query timeout (seconds) |
| **pool_size** | integer | 8 | 1-20 | Connection pool size |

### Table Schema Contract

| Column | Type | Nullable | Default | Description |
|--------|------|:--------:|---------|-------------|
| **domain** | String | ❌ | - | Data domain |
| **table_name** | String | ❌ | - | Source table name |
| **partition_date** | Date | ❌ | - | Partition date (ds value) |
| **file_path** | String | ❌ | - | S3 object key |
| **record_count** | UInt64 | ❌ | 0 | Number of records |
| **file_size** | UInt64 | ❌ | 0 | File size in bytes |
| **correlation_id** | String | ❌ | - | Tracking UUID |
| **processed_at** | DateTime | ❌ | now() | Processing timestamp |

### Insert Operations

| Operation | SQL Pattern | Batch Size | Performance Target |
|-----------|-------------|:----------:|:-----------------:|
| **Single Insert** | `INSERT INTO <table> VALUES (...)` | 1 record | < 100ms |
| **Batch Insert** | `INSERT INTO <table> VALUES (...), (...)` | 5-10 records | < 500ms |
| **Bulk Insert** | `INSERT INTO <table> SELECT * FROM s3(...)` | File-based | < 30s |

### Error Handling

| Error Type | ClickHouse Code | Action | Recovery |
|------------|:---------------:|--------|----------|
| **Connection Failed** | 210 | Retry with backoff | Reconnect |
| **Authentication** | 516 | Fail immediately | Check credentials |
| **Table Not Found** | 60 | Fail immediately | Verify schema |
| **Timeout** | - | Retry with smaller batch | Reduce batch size |
| **Disk Full** | 243 | Retry after delay | Wait for cleanup |

## Security Contracts

### IAM Role Permissions

| Service | Actions | Resource Pattern | Condition |
|---------|---------|------------------|-----------|
| **SQS** | `ReceiveMessage`, `DeleteMessage` | `arn:aws:sqs:<region>:<account>:<env>-*-load-queue` | None |
| **S3** | `GetObject` | `arn:aws:s3:::<curated-bucket>/<domain>/*` | None |
| **Secrets Manager** | `GetSecretValue` | `arn:aws:secretsmanager:<region>:<account>:secret:<env>/clickhouse/*` | None |
| **CloudWatch** | `PutMetricData` | `*` | Namespace: `Load/Pipeline` |

### Network Security

| Component | Network Access | Security Group | Port |
|-----------|----------------|----------------|:----:|
| **Lambda Function** | Private subnets | `sg-lambda-load` | Outbound only |
| **ClickHouse** | Private network | `sg-clickhouse` | 9000, 9440 |
| **NAT Gateway** | Public subnet | Default | 80, 443 |

### Data Encryption

| Data Type | Encryption Method | Key Management | Scope |
|-----------|-------------------|----------------|-------|
| **SQS Messages** | SSE-SQS | AWS managed | All messages |
| **S3 Objects** | SSE-S3 | AWS managed | Curated bucket |
| **ClickHouse Connection** | TLS 1.2+ | Certificate-based | All connections |
| **Secrets** | AWS Secrets Manager | AWS KMS | Database credentials |

## Performance Contracts

### SLA Requirements

| Metric | Target | Measurement | Alerting |
|--------|:------:|-------------|----------|
| **Processing Latency** | < 5 minutes | End-to-end | > 10 minutes |
| **Throughput** | 1000 msgs/hour/domain | Per domain | < 500 msgs/hour |
| **Availability** | 99.9% | Monthly | < 99.5% |
| **Error Rate** | < 1% | Hourly average | > 5% |

### Resource Limits

| Resource | Limit | Monitoring | Scaling Action |
|----------|:-----:|------------|----------------|
| **Lambda Memory** | 1024MB | CloudWatch | Increase if >85% |
| **Lambda Timeout** | 300s | CloudWatch | Optimize or increase |
| **SQS Queue Depth** | 100 messages | CloudWatch | Scale Lambda concurrency |
| **ClickHouse Connections** | 8 per Lambda | Application logs | Increase pool size |

## Validation Contracts

### Message Validation

| Field | Validation Rule | Error Code | Action |
|-------|----------------|:----------:|--------|
| **bucket** | Required, S3 bucket format | PARSE_ERROR | DLQ |
| **key** | Required, S3 key format | PARSE_ERROR | DLQ |
| **domain** | Required, alphanumeric | VALIDATION_ERROR | DLQ |
| **table_name** | Required, alphanumeric + underscore | VALIDATION_ERROR | DLQ |
| **partition** | Required, ds=YYYY-MM-DD format | VALIDATION_ERROR | DLQ |
| **correlation_id** | Required, UUID v4 format | VALIDATION_ERROR | DLQ |

### File Validation

| Check | Validation | Error Handling | Recovery |
|-------|------------|----------------|----------|
| **File Exists** | S3 HEAD request | Retry 3 times | Manual check |
| **File Size** | > 1KB, < 1GB | Log warning/error | Process if possible |
| **File Format** | .parquet extension | Reject immediately | DLQ |
| **File Readable** | Parquet metadata read | Retry 2 times | Manual fix |

## Integration Testing

### Test Scenarios

| Scenario | Components | Input | Expected Output | Validation |
|----------|------------|-------|----------------|------------|
| **Happy Path** | SQS→Lambda→ClickHouse | Valid message | Successful insert | Data in DW |
| **Parse Error** | SQS→Lambda | Invalid JSON | Error response | Message in DLQ |
| **Connection Error** | Lambda→ClickHouse | Valid data | Retry then DLQ | Connection recovery |
| **Large File** | Full pipeline | 100MB+ file | Successful processing | Memory management |
| **Batch Processing** | SQS batch | 10 messages | Partial success handling | Error isolation |