# Load Pipeline Specification

| 항목 | 내용 |
|------|------|
| 아키텍처 | Curated S3 이벤트 → EventBridge Rule → Load Event Publisher Lambda → 도메인별 SQS |
| 데이터 소비자 | (외부) ClickHouse 로더 — 저장소 비포함, `load_contracts.py` 기준으로 구현 필요 |
| 코드 기준 | `infrastructure/pipelines/load/load_pipeline_stack.py`, `src/lambda/functions/load_event_publisher/handler.py` |

## 구성요소 요약

| Stage | Component | 설명 |
|-------|-----------|------|
| Trigger | EventBridge Rule | Curated 버킷 ObjectCreated 이벤트 필터 |
| Transform | Load Event Publisher Lambda | 이벤트 검증 및 SQS 메시지 발행 |
| Queue | `{env}-{domain}-load-queue` | 온프레미스 로더 폴링 대상 |
| DLQ | `{env}-{domain}-load-dlq` | `max_receive_count` 초과 메시지 보관 (14일) |

## 도메인별 설정 (`load_domain_configs`)

| 키 | 예시 | 설명 |
|----|------|------|
| `domain` | `market` | 큐·DLQ·규칙 이름 구성 요소 |
| `s3_prefix` | `market/` | EventBridge Prefix 필터 |
| `priority` | `1` | SQS 메시지 속성(문자열) |
| `visibility_timeout_seconds` | `1800` | 메인 큐 가시성 타임아웃 |
| `message_retention_days` | `14` | 메시지 보관 기간 |
| `max_receive_count` | `3` | DLQ 이동 전 재시도 횟수 |

## S3 Event 패턴

| 항목 | 패턴 | 설명 |
|------|-------|------|
| `detail-type` | `"Object Created"` | 객체 생성 이벤트만 처리 |
| `bucket.name` | `<curated-bucket>` | Curated 버킷 필터 |
| `object.key` | Prefix `<domain>/` | 도메인별 필터 |
| `object.size` | `>= MIN_FILE_SIZE_BYTES` (기본 1024) | 빈 파일 방지 |

### 메시지 변환 (EventBridge → SQS)

| 출력 필드 | 소스 | 필수 | 예시 | 비고 |
|-----------|------|:---:|------|------|
| `bucket` | `detail.bucket.name` | ✅ | `data-pipeline-curated-dev` | Curated 버킷 |
| `key` | `detail.object.key` | ✅ | `market/prices/ds=2025-09-10/part-001.parquet` | 현재 구현이 기대하는 패턴 |
| `domain` | 경로 파싱 | ✅ | `market` | 첫 세그먼트 |
| `table_name` | 경로 파싱 | ✅ | `prices` | 두 번째 세그먼트 |
| `partition` | 경로 파싱 | ✅ | `ds=2025-09-10` | 세 번째 세그먼트 |
| `file_size` | `detail.object.size` | ❌ | `1048576` | 존재 시 정수 검증 |
| `correlation_id` | UUID v4 생성 | ✅ | `550e...` | 추적용 |
| `presigned_url` | - | ❌ | - | 현재 생성하지 않음 |

> Transform 출력 경로는 `interval/.../layer=...` 구조이므로, Load 파이프라인을 활성화하려면 키 파싱 규칙을 정합화해야 합니다.

### SQS 메시지 속성

| Attribute | 값 (string) | 설명 |
|-----------|--------------|------|
| `ContentType` | `application/json` | 메시지 본문 형식 |
| `Domain` | `<domain>` | 라우팅 메타 |
| `TableName` | `<table_name>` | 라우팅 메타 |
| `Priority` | `PRIORITY_MAP[domain]` | 환경 설정 기반 문자열 |

## 오류 처리 및 DLQ

| 항목 | 현행 동작 |
|------|-----------|
| Lambda ValidationError | 이벤트 스킵 (SQS 미발행) |
| Loader 실패 | 온프레미스 로더가 재시도/ACK 구현 필요 |
| DLQ 정책 | `max_receive_count` 초과 시 `{env}-{domain}-load-dlq`로 이동 (14일 보관) |

## 모니터링

| 항목 | 상태 |
|------|------|
| CloudWatch 알람 | 기본 미제공 (QueueDepth/DLQ 메시지 알람을 별도 구성 필요) |
| Lambda 로깅 | ValidationError 및 스킵 사유를 INFO/WARNING 레벨로 기록 |

## 온프레미스 Loader 계약 참고 (`load_contracts.py`)

| 구성 요소 | 주요 필드 | 설명 |
|-----------|-----------|------|
| `LoadMessage` | `bucket`, `key`, `domain`, `table_name`, `partition`, `correlation_id`, `file_size?`, `presigned_url?` | 수신 메시지 스키마 검증 |
| `LoaderConfig` | `queue_url`, `wait_time_seconds`, `max_messages`, `visibility_timeout`, `query_timeout`, `backoff_seconds` | 폴링 및 재시도 설정 검증 |
| 메시지 속성 | `Priority` 등 | SQS MessageAttributes로 제공 |

## 보안 및 네트워킹

| 항목 | 설명 |
|------|------|
| Lambda 권한 | SQS `SendMessage` (도메인 큐), S3 `GetObject` 권한 불필요 |
| Loader 권한 | `ReceiveMessage`, `DeleteMessage`, `ChangeMessageVisibility`, `s3:GetObject` |
| 네트워크 | 온프레미스 → AWS outbound (S3/SQS HTTPS). 인바운드 포트 필요 없음 |
| 데이터 암호화 | 전송: TLS 1.2+, 저장: S3/SQS SSE-S3/KMS |

## 진행 중인 과제

| 항목 | 설명 |
|------|------|
| 경로 정합성 | Transform 출력(`interval/.../layer=`)과 Load 파서(`ds=` 기대)를 통일 |
| Loader 구현 | 저장소에 미포함 — ClickHouse 로더, 재시도, 모니터링 로직 별도 개발 필요 |
| 테스트 보강 | `tests/unit/load` 영역에 Lambda/계약 검증 테스트 추가 필요 |

---

| 관련 문서 | 경로 |
|----------|------|
| Load Component Contracts | `docs/specs/load/load-component-contracts.md` |
| Extract Transform 명세 | `docs/specs/transform/state-machine-contract.md` |
