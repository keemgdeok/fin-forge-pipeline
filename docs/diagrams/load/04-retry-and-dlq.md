# Load Retry & DLQ Handling

```mermaid
sequenceDiagram
  autonumber
  participant SQS as SQS Queue
  participant LOAD as Load Lambda
  participant DW as Data Warehouse
  participant DLQ as Dead Letter Queue
  participant SNS as SNS Topic

  Note over SQS,SNS: Load 실패 처리 플로우
  
  SQS->>LOAD: SQS Event (message)
  LOAD->>DW: INSERT/UPSERT Batch
  alt DW Connection/Query 실패
    DW-->>LOAD: Error Response
    LOAD->>LOAD: Lambda 내부 재시도 (3회)
    Note over LOAD: Exponential Backoff:<br/>2s → 4s → 8s
    
    alt Lambda 재시도 성공
      LOAD->>DW: Retry Success
      LOAD->>SQS: Delete Message
    else Lambda 재시도 실패
      LOAD-->>SQS: batchItemFailures 반환
      SQS->>SQS: receiveCount++
      Note over SQS: VisibilityTimeout 후<br/>SQS 레벨 재처리
      
      alt receiveCount ≤ maxReceiveCount(3)
        Note over SQS: SQS가 메시지 재시도
      else receiveCount > maxReceiveCount(3)
        SQS->>DLQ: Move to DLQ
        DLQ->>SNS: DLQ 알람 발송
        Note over SNS: 운영팀 즉시 대응 필요<br/>06-monitoring 참조
      end
    end
  else DW 성공
    DW-->>LOAD: Success Response  
    LOAD->>SQS: Delete Message
  end

  Note over DLQ: DLQ 메시지 수동 재처리<br/>또는 데이터 품질 이슈 분석
```

## 재시도 전략 상세

### **Lambda 내부 재시도**
- **횟수**: 3회 (총 4회 시도)
- **백오프**: Exponential (2초 → 4초 → 8초)
- **대상 오류**: Connection timeout, temporary DW errors

### **SQS 레벨 재시도**
- **maxReceiveCount**: 3 (총 3회 SQS 재처리)
- **VisibilityTimeout**: Lambda timeout × 6 (30분)
- **redrive 정책**: 자동 DLQ 이동

### **DLQ 처리**
- **즉시 알람**: SNS → Slack/Email
- **수동 처리**: 운영팀 개입 필요
- **재처리**: DLQ → 원본 Queue 수동 이동

참조: 03-sequence.md (정상 플로우), 06-monitoring-metrics.md (알람 설정)