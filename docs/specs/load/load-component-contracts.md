# Load Component Contracts (Pull Model)

Load 파이프라인의 Pull 아키텍처 구성요소 계약입니다. SQS, 온프레미스 ClickHouse 로더 에이전트(이하 "Loader"), ClickHouse 간 데이터 계약과 연결 설정을 정의합니다.

## SQS Message Contracts

### Message Schema

| Field | Type | Required | Format | Example |
|-------|------|:--------:|--------|---------|
| **bucket** | string | ✅ | S3 bucket name | `data-pipeline-curated-dev` |
| **key** | string | ✅ | S3 object key | `market/prices/adjusted/ds=2025-09-10/part-001.parquet` |
| **domain** | string | ✅ | [a-z0-9-]{1,50} | `market` |
| **table_name** | string | ✅ | [a-z0-9_]{1,50} | `prices` |
| **partition** | string | ✅ | ds=YYYY-MM-DD | `ds=2025-09-10` |
| **file_size** | integer | ❌ | > 0 | `1048576` |
| **correlation_id** | string | ✅ | UUID v4 | `550e8400-e29b-41d4-a716-446655440000` |
| **presigned_url** | string | ❌ | HTTPS URL | `https://...X-Amz-Signature=...` |

### Message Attributes

| Attribute | Type | Purpose | Example |
|-----------|------|---------|---------|
| **ContentType** | String | Message format | `application/json` |
| **Domain** | String | Routing filter | `market` |
| **TableName** | String | Table filter | `prices` |
| **Priority** | Number | Processing order | `1` (High), `2` (Medium), `3` (Low) |

비고:
- 보안 강화를 위해 S3 접근에 프리사인드 URL(`presigned_url`)을 선택적으로 포함할 수 있습니다. 포함 시 Loader는 URL을 우선 사용하고, 미포함 시 IAM 자격증명으로 접근합니다.

### Queue Configuration

| Setting | Main Queue | Dead Letter Queue | Description |
|---------|:----------:|:----------------:|-------------|
| **Name Pattern** | `<env>-<domain>-load-queue` | `<env>-<domain>-load-dlq` | Environment and domain scoped |
| **Visibility Timeout** | 1800s | 300s | 6× and 1× per‑message timeout |
| **Message Retention** | 14 days | 14 days | Maximum retention |
| **Max Receive Count** | 3 | Unlimited | Retry limit before DLQ |

## Loader Agent Contracts (On‑prem)

### Input Contract (SQS API)

| Parameter | Source | Type | Validation |
|-----------|--------|------|------------|
| **Messages** | `ReceiveMessage` | Array[SQSMessage] | 1–10 per call (long poll) |
| **Message.Body** | Message body | JSON string | Valid JSON schema |
| **Message.Attributes** | SQS attributes | Map[string, any] | Optional |
| **ReceiptHandle** | SQS handle | string | Required for deletion |

### Processing & Acknowledgement

| Operation | API | Behavior | Notes |
|-----------|-----|---------|-------|
| ACK success | `DeleteMessage/Batch` | Delete processed messages | 개별/배치 삭제 지원 |
| Defer retry | `ChangeMessageVisibility` | Extend visibility timeout | 일시 오류 시 백오프 |
| Fail (no ACK) | — | Let visibility expire | `receiveCount` 증가 후 재전달 |

### Error Codes (Agent semantics)

| Code | Description | Retry Action | DLQ Action |
|------|-------------|-------------|-----------|
| **PARSE_ERROR** | Invalid JSON in message body | No retry | Move after maxReceiveCount |
| **VALIDATION_ERROR** | Schema validation failure | No retry | Move after maxReceiveCount |
| **FILE_NOT_FOUND** | S3 object not accessible | Retry 3× (backoff) | Move after exhaustion |
| **CONNECTION_ERROR** | ClickHouse connection failure | Retry 3× (backoff) | Move after exhaustion |
| **TIMEOUT_ERROR** | Query timeout | Retry 2× (backoff) | Move after exhaustion |

### Loader Configuration (Environment/Config)

| Variable | Format | Required | Example | Description |
|----------|--------|:--------:|---------|-------------|
| **AWS_REGION** | string | ✅ | `ap-northeast-2` | AWS SDK region |
| **SQS_QUEUE_URL** | url | ✅ | `https://sqs.../env-domain-load-queue` | Target queue |
| **SQS_WAIT_TIME_SECONDS** | integer | ✅ | `20` | Long poll wait time |
| **SQS_MAX_MESSAGES** | integer | ✅ | `10` | Max messages per poll |
| **SQS_VISIBILITY_TIMEOUT** | integer | ✅ | `1800` | Seconds (≥ 6× query timeout) |
| **AWS_ACCESS_KEY_ID** | string | C | `AKIA...` | If using IAM user |
| **AWS_SECRET_ACCESS_KEY** | string | C | `****` | If using IAM user |
| **CLICKHOUSE_HOST** | host:port | ✅ | `localhost:9000` | Local/nearby CH endpoint |
| **CLICKHOUSE_DATABASE** | string | ✅ | `data_warehouse` | Target database |
| **CLICKHOUSE_USER** | string | ✅ | `loader_user` | Database username |
| **CLICKHOUSE_PASSWORD** | string | C | `****` | If required |
| **QUERY_TIMEOUT** | integer | ✅ | `30` | Seconds per insert |
| **WORKER_CONCURRENCY** | integer | ✅ | `8` | Parallel workers/threads |
| **LOG_LEVEL** | enum | ❌ | `INFO` | Logging verbosity |

## ClickHouse Integration (S3 Pull)

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

### Insert Operations (via `s3` table function)

| Operation | SQL Pattern | Input | Performance Target |
|-----------|-------------|:-----:|:-----------------:|
| **Single file** | `INSERT INTO <table> SELECT * FROM s3('<https-url>', '<AKIA>', '<SECRET>', 'Parquet')` | 1 file | < 10s |
| **Multi‑file (wildcard)** | `INSERT INTO <table> SELECT * FROM s3('<https-prefix>/*.parquet', '<AKIA>', '<SECRET>', 'Parquet')` | N files | < 30s |
| **Partition range** | `INSERT ... SELECT * FROM s3('.../ds=<YYYY-MM-DD>/*.parquet', ...)` | Partition | < 30s |

비고:
- 퍼블릭 S3 엔드포인트(HTTPS) 사용. 필요 시 사설 경로/프록시 구성.
- 권장 CH 설정(예): 적절한 병렬성, Parquet 스키마 자동 매핑 옵션.

### Error Handling

| Error Type | ClickHouse Code | Action | Recovery |
|------------|:---------------:|--------|----------|
| **Connection Failed** | 210 | Retry with backoff | Reconnect |
| **Authentication** | 516 | Fail immediately | Check credentials |
| **Table Not Found** | 60 | Fail immediately | Verify schema |
| **Timeout** | - | Retry with smaller batch | Reduce batch size |
| **Disk Full** | 243 | Retry after delay | Wait for cleanup |

## Security Contracts

### IAM Role Permissions (Least privilege)

| Service | Actions | Resource Pattern | Condition |
|---------|---------|------------------|-----------|
| **SQS** | `ReceiveMessage`, `DeleteMessage`, `ChangeMessageVisibility` | `arn:aws:sqs:<region>:<account>:<env>-*-load-queue` | None |
| **S3** | `GetObject` | `arn:aws:s3:::<curated-bucket>/<domain>/*` | None |
| **Secrets Manager** | `GetSecretValue` | `arn:aws:secretsmanager:<region>:<account>:secret:<env>/clickhouse/*` | None |
| **CloudWatch** | `PutMetricData` | `*` | Namespace: `Load/Pipeline` |

### Network Security

| Component | Network Access | Security Group | Port |
|-----------|----------------|----------------|:----:|
| **On‑prem Loader** | Outbound to AWS | Allowlist | 443 (HTTPS) |
| **ClickHouse** | Local/Datacenter | Internal policy | 9000, 9440 |
| **Internet/NAT** | Datacenter egress | Corp policy | 80, 443 |

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
| **Worker Threads** | 32 | Agent metrics | Increase if backlog ↑ |
| **Per‑message Timeout** | 300s | Agent metrics | Tune per dataset |
| **SQS Queue Depth** | 100 messages | CloudWatch | Increase workers |
| **ClickHouse Connections** | 8 per agent | Agent logs | Increase cautiously |

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
| **Happy Path** | SQS→On‑prem Loader→ClickHouse | Valid message | Successful insert | Data in DW |
| **Parse Error** | SQS→Loader | Invalid JSON | Not deleted | Message in DLQ |
| **Connection Error** | Loader→ClickHouse | Valid data | Retry then DLQ | Connection recovery |
| **Large File** | Full pipeline | 100MB+ file | Successful processing | Memory management |
| **Batch Processing** | SQS batch | 10 messages | Per‑message ACK | Error isolation |
