# Load Retry & DLQ Handling (Pull Model)

```mermaid
sequenceDiagram
  autonumber
  participant SQS as SQS Queue
  participant AGT as On‑prem CH Loader
  participant CH as ClickHouse
  participant DLQ as Dead Letter Queue
  participant SNS as SNS Topic

  Note over SQS,SNS: Load 실패 처리 플로우
  
  SQS->>AGT: ReceiveMessage (1 msg)
  AGT->>CH: INSERT ... SELECT s3(...)
  alt CH Connection/Query 실패
    CH-->>AGT: Error
    AGT->>AGT: 내부 재시도 (3회)
    Note over AGT: Exponential Backoff:<br/>2s → 4s → 8s
    
    alt 재시도 성공
      AGT->>CH: Retry Success
      AGT->>SQS: DeleteMessage
    else 재시도 실패
      AGT-->>SQS: Delete 생략 (가시성 유지)
      SQS->>SQS: VisibilityTimeout 종료 → 재전달<br/>receiveCount++
      
      alt receiveCount ≤ maxReceiveCount(3)
        Note over SQS: SQS가 메시지 재시도
      else receiveCount > maxReceiveCount(3)
        SQS->>DLQ: Move to DLQ
        DLQ->>SNS: DLQ 알람 발송
        Note over SNS: 운영팀 즉시 대응 필요<br/>06-monitoring 참조
      end
    end
  else CH 성공
    CH-->>AGT: Insert OK
    AGT->>SQS: DeleteMessage (ACK)
  end

  Note over DLQ: DLQ 메시지 수동 재처리<br/>또는 데이터 품질 이슈 분석
```

## 재시도 전략 상세

### **Loader 내부 재시도**
- **횟수**: 3회 (총 4회 시도)
- **백오프**: Exponential (2초 → 4초 → 8초)
- **대상 오류**: CH 연결/일시 쿼리 실패, S3 일시적 오류

### **SQS 레벨 재시도**
- **maxReceiveCount**: 3 (총 3회 재전달 후 DLQ)
- **VisibilityTimeout**: per‑message timeout × 6 (권장: 1800초)
- **redrive 정책**: 초과 시 자동 DLQ 이동

### **DLQ 처리**
- **즉시 알람**: SNS → Slack/Email
- **수동 처리**: 운영팀 개입 필요
- **재처리**: DLQ → 원본 Queue 수동 이동

참조: 03-sequence.md (정상 플로우), 06-monitoring-metrics.md (알람 설정)
