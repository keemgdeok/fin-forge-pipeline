# Extract Pipeline — 데이터 수집 파이프라인 명세

| 항목 | 내용 |
|------|------|
| 문서 목적 | Extract 단계 아키텍처, 구성요소, 성능·운영 파라미터 표준화 |
| 코드 기준 | `infrastructure/pipelines/daily_prices_data/ingestion_stack.py`, `src/lambda/functions/ingestion_*` |
| 데이터 흐름 | EventBridge 스케줄 → Orchestrator → SQS → Worker → RAW S3 |

## 아키텍처 개요

| 컴포넌트 | 역할 | 트리거 | 출력 |
|----------|------|--------|------|
| EventBridge Schedule | 정시 실행 | Cron 표현식 | Orchestrator 호출 |
| Orchestrator Lambda | 작업 분할 | EventBridge | SQS 메시지, 배치 트래커 초기화 |
| SQS Queue | 비동기 큐 | Orchestrator | Worker 트리거 |
| Worker Lambda | 데이터 수집 | SQS 이벤트 | RAW 객체, 매니페스트, 배치 트래커 업데이트 |

## 지원 트리거 패턴

| 패턴 | 트리거 | 용도 | 비고 |
|------|--------|------|------|
| Scheduled | EventBridge Cron | 일일 정기 수집 | 환경별 기본 이벤트 사용 |
| Event-Driven | _(미구현)_ | 설계만 존재 | 추후 구현 시 업데이트 필요 |
| Manual | Lambda 직접 호출 | 재처리/테스트 | 기본 payload 오버라이드 |

## 성능 및 설정

| 설정 항목 | 값 | 설명 |
|-----------|-----|------|
| Orchestrator Memory | `lambda_memory` (환경 설정) | 경량 팬아웃 |
| Worker Memory | `worker_memory` | 외부 API 호출 처리 |
| Chunk Size | `orchestrator_chunk_size` | 심볼 그룹화 (Dev 10) |
| SQS Batch | `sqs_batch_size` | Worker 이벤트 배치 (Dev 1) |
| Visibility Timeout | `worker_timeout × 6` | 실패 재처리 대비 |
| DLQ 이동 기준 | `max_retries` | Dev 기본 5회 |

## 모니터링 알람 기준

| 지표 | 임계값 (Dev 기준) | 평가 기간 | 대응 |
|------|-----------------|-----------|------|
| Queue Depth | 100 메시지 | 5분 | 처리량/구성 점검 |
| ApproximateAgeOfOldestMessage | 300초 | 5분 | Worker 오류 확인 |
| Lambda Errors | ≥ 1 | 5분 | 즉시 조사 |

## 도메인 지원

| 도메인 | 데이터 소스 | 스케줄 | 큐/DLQ |
|--------|------------|--------|--------|
| **daily-prices-data** | Yahoo Finance | 07:00 KST (UTC 22:00) | 전용 큐 |

## Transform 연계

| 단계 | 담당 | 설명 |
|------|------|------|
| 매니페스트 생성 | Worker Lambda | `_batch.manifest.json`을 RAW 버킷에 기록 |
| 실행 책임 | Runner / Ops | `manifest_keys` 입력 생성 후 Step Functions 실행 |
| 자동 트리거 | _(미구현)_ | EventBridge 규칙 부재, 향후 확장 필요 |

---

| 참고 문서 | 경로 |
|----------|------|
| 트리거 상세 | `docs/specs/extract/event-driven-triggers.md` |
| 오케스트레이터 계약 | `docs/specs/extract/orchestrator-contract.md` |
| 워커 계약 | `docs/specs/extract/worker-contract.md` |
