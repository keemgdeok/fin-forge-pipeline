# Load Diagrams Overview

#### [Components]
<img src="01-components-1.svg" alt="Components" style="width:100%;" />
- EventBridge → Lambda → SQS 구성과 external-loader 위치

<br>

#### [Data Flow]
<img src="02-data-flow-1.svg" alt="Data Flow" style="width:50%;" />
- Curated Event에서 ClickHouse Load까지의 흐름(external-loader 전제)

<br>

#### [Sequence]
<img src="03-sequence-1.svg" alt="Sequence" style="width:100%;" />
- Publisher → SQS → Loader 시퀀스

<br>

#### [Retry &amp; DLQ]
<img src="04-retry-and-dlq-1.svg" alt="Retry &amp; DLQ" style="width:100%;" />
- 재시도 및 DLQ 처리 개요

<br>


