# Extract Pipeline — 데이터 수집 파이프라인 명세

본 문서는 Extract 단계의 아키텍처, 컴포넌트 계약, 그리고 데이터 플로우를 정의합니다. Extract는 외부 데이터를 Raw S3에 저장하는 첫 단계입니다.

## 아키텍처 개요

Extract는 **Event-Driven Fan-Out 패턴**으로 확장 가능한 수집을 구현합니다.

| 컴포넌트 | 역할 | 트리거 | 출력 |
|----------|------|--------|------|
| **EventBridge Schedule** | 정시 실행 | Cron 표현식 | Orchestrator 호출 |
| **Orchestrator Lambda** | 작업 분할 | EventBridge | SQS 메시지 |
| **SQS Queue** | 비동기 큐 | Orchestrator | Worker 트리거 |
| **Worker Lambda** | 데이터 수집 | SQS 이벤트 | S3 저장 |

## 지원 패턴

| 패턴 | 트리거 | 용도 | 예시 |
|------|--------|------|------|
| **Scheduled** | EventBridge Cron | 정기 수집 | 매일 22:00 KST |
| **Event-Driven** | EventBridge 이벤트 | 즉시 처리 | 신규 심볼 추가 |
| **Manual** | 직접 호출 | 재처리/테스트 | 백필 작업 |

## 성능 및 설정

### 처리 용량
| 설정 | 값 | 설명 |
|------|-----|------|
| Orchestrator Memory | 256MB | 경량 팬아웃 |
| Worker Memory | 512MB+ | API 호출/처리 |
| Chunk Size | 5개 | 심볼 그룹화 |
| SQS Batch | 최대 10개 | 배치 전송 |
| SQS Visibility | Worker Timeout × 6 | 재처리 방지 |
| DLQ 이동 | 5회 실패 후 | 격리 |

### 모니터링 알람
| 지표 | 임계값 | 평가 시간 | 조치 |
|------|--------|-----------|------|
| Queue Depth | 100개 | 5분 | 용량 확인 |
| Message Age | 300초 | 5분 | Worker 점검 |
| Lambda Errors | 1개 | 5분 | 즉시 조사 |

## 도메인 지원

각 도메인은 독립적인 스택으로 배포됩니다.

| 도메인 | 데이터 소스 | 스케줄 | 큐/DLQ |
|--------|------------|--------|--------|
| **daily-prices-data** | Yahoo Finance | 22:00 UTC | 전용 큐 |

## Transform 연계

| 단계 | 트리거 | 메타데이터 |
|------|--------|----------|
| S3 Event | Raw 저장 완료 | 파티션 정보 |
| EventBridge | Transform 시작 | 데이터 위치 |

---

*본 명세는 `infrastructure/pipelines/daily_prices_data/ingestion_stack.py` 기반입니다.*
