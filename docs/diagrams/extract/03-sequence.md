```mermaid
sequenceDiagram
  autonumber
  participant EV as EventBridge Rule
  participant ORC as Orchestrator Lambda
  participant DDB as DynamoDB Batch Tracker
  participant SQS as SQS Queue
  participant WRK as Worker Lambda
  participant PRV as Market Data Provider
  participant S3 as RAW Bucket
  participant TRG as Processing Trigger Lambda
  participant SF as Transform Step Functions

  EV->>ORC: Scheduled event
  ORC->>ORC: Load symbol universe (SSM 또는 S3)
  ORC->>DDB: PutItem expected_chunks
  ORC->>SQS: SendMessageBatch(chunks of symbols)

  par parallel workers (per SQS message)
    SQS->>WRK: Invoke with chunk payload
    WRK->>PRV: fetch_prices(symbols)
    PRV-->>WRK: List<PriceRecord>
    loop per symbol/day
      WRK->>S3: head_object(interval/...)
      alt Object exists
        WRK-->>WRK: Skip write (idempotent)
      else
        WRK->>S3: PutObject(symbol partition, optional .gz)
      end
    end
      WRK->>DDB: UpdateItem ADD processed_chunks
      alt processed_chunks < expected
        WRK-->>SQS: ack message
      else
        WRK->>S3: PutObject partition manifest(s)
      Note over WRK,S3: 파티션 매니페스트 업로드 후 트리거가 실행 조건 확인
    end
  and
    Note over SQS: Messages redelivered until chunk success or DLQ
  end

  WRK->>DDB: UpdateItem status=complete
  DDB-->>TRG: Stream event (status change)
  TRG->>S3: collect_manifest_entries(batch_id)
  TRG->>SF: StartExecution(manifest_keys)
  Note over TRG,SF: DynamoDB Stream trigger가 SFN 실행
```
