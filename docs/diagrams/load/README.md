# Load Diagrams Overview

| Diagram | 미리보기 | 설명 |
|---------|-----------|------|
| Components | <img src="01-components-1.svg" width="320" alt="Components" /> | EventBridge → Lambda → SQS 구성 및 외부 로더 위치 |
| Data Flow | <img src="02-data-flow-1.svg" width="320" alt="Data Flow" /> | Curated 이벤트에서 ClickHouse 적재까지의 흐름 (외부 로더 전제) |
| Sequence | <img src="03-sequence-1.svg" width="320" alt="Sequence" /> | Publisher → SQS → Loader 시퀀스 |
| Retry & DLQ | <img src="04-retry-and-dlq-1.svg" width="320" alt="Retry & DLQ" /> | 재시도·DLQ 처리 개요 |
| Batch Optimization | <img src="05-batch-optimization-1.svg" width="320" alt="Batch" /> | 폴링/배치/쿼리 최적화 가이드 |
| Monitoring Metrics | <img src="06-monitoring-metrics-1.svg" width="320" alt="Monitoring" /> | 운영 시 모니터링해야 할 핵심 지표 |
