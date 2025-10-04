# Load Diagrams Overview

| Diagram | 미리보기 | 설명 |
|---------|-----------|------|
| Components | ![Components](01-components-1.svg) | EventBridge → Lambda → SQS 구성 및 외부 로더 위치 |
| Data Flow | ![Data Flow](02-data-flow-1.svg) | Curated 이벤트에서 ClickHouse 적재까지의 흐름 (외부 로더 전제) |
| Sequence | ![Sequence](03-sequence-1.svg) | Publisher → SQS → Loader 시퀀스 |
| Retry & DLQ | ![Retry & DLQ](04-retry-and-dlq-1.svg) | 재시도·DLQ 처리 개요 |
| Batch Optimization | ![Batch](05-batch-optimization-1.svg) | 폴링/배치/쿼리 최적화 가이드 |
| Monitoring Metrics | ![Monitoring](06-monitoring-metrics-1.svg) | 운영 시 모니터링해야 할 핵심 지표 |

