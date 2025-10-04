# Orchestrator Lambda — I/O 계약 명세

| 항목 | 내용 |
|------|------|
| 책임 | EventBridge 스케줄 이벤트를 도메인별 SQS 메시지로 팬아웃 |
| 코드 기준 | `src/lambda/functions/ingestion_orchestrator/handler.py` |
| 배포 리소스 | `{environment}-daily-prices-data-orchestrator`, EventBridge Rule |

## 기본 설정

| 구성 요소 | 값 |
|----------|-----|
| Runtime | Python 3.12 |
| Memory | 256MB |
| Timeout | 60초 |
| Trigger | EventBridge Rule (Schedule) |

## 입력 계약

| 필드 | 타입 | 필수 | 기본값 | 예시 | 설명 |
|------|------|:---:|--------|------|------|
| data_source | string | ❌ | `yahoo_finance` | `yahoo_finance` | 데이터 소스 식별자 |
| data_type | string | ❌ | `prices` | `prices` | 데이터 타입 |
| domain | string | ❌ | `market` | `market` | 비즈니스 도메인 |
| table_name | string | ❌ | `prices` | `prices` | 대상 테이블 |
| symbols | array | ⚠️ | `[]` | `["AAPL", "MSFT"]` | 심볼 목록 (없으면 외부 로딩) |
| period | string | ❌ | `1mo` | `1mo` | 조회 기간 |
| interval | string | ❌ | `1d` | `1d` | 시간 해상도 |
| file_format | string | ❌ | `json` | `json` | `parquet` 요청 시에도 JSON으로 저장 |
| correlation_id | string | ❌ | `None` | `manual-001` | 로그 추적용 상관 키 |

## 환경변수

| 변수명 | 필수 | 기본값 | 설명 |
|--------|:---:|--------|------|
| ENVIRONMENT | ✅ | - | 배포 환경 식별자 |
| QUEUE_URL | ✅ | - | 대상 SQS 큐 URL |
| CHUNK_SIZE | ❌ | config 값 | 심볼 묶음 크기 (Dev 10) |
| SQS_SEND_BATCH_SIZE | ❌ | `10` | SQS `SendMessageBatch` 크기 |
| SYMBOLS_SSM_PARAM | ❌ | - | 심볼 리스트가 저장된 SSM 파라미터 |
| SYMBOLS_S3_BUCKET | ❌ | 아티팩트 버킷 | 심볼 파일이 위치한 버킷 |
| SYMBOLS_S3_KEY | ❌ | - | 심볼 파일 키 |
| BATCH_TRACKING_TABLE | ❌ | - | DynamoDB 배치 트래커 테이블 이름 |
| BATCH_TRACKER_TTL_DAYS | ❌ | `7` | 배치 트래커 항목 TTL |

## 심볼 로딩 우선순위

| 순위 | 소스 | 제어 키 | 비고 |
|:---:|------|---------|------|
| 1 | SSM Parameter | `SYMBOLS_SSM_PARAM` | 중앙 관리, 암호화 |
| 2 | S3 Object | `SYMBOLS_S3_BUCKET` + `SYMBOLS_S3_KEY` | 버킷 미설정 시 아티팩트 버킷 사용 |
| 3 | 이벤트 payload | `event.symbols` | 온디맨드 심볼 지정 |
| 4 | Fallback | 고정 `["AAPL"]` | 모든 소스 실패 시 keep-alive |

## 성공 응답 필드

| 필드 | 타입 | 예시 | 설명 |
|------|------|------|------|
| published | integer | `24` | 발행된 SQS 메시지 수 |
| chunks | integer | `8` | 생성된 청크 수 |
| environment | string | `dev` | 실행 환경 |
| batch_id | string | `c7d3d6c8-...` | 배치 트래커 키 |
| batch_ds | string | `2025-09-07` | 배치 기준 날짜 |

### SQS 메시지 구조

| 필드 | 타입 | 예시 | 설명 |
|------|------|------|------|
| data_source | string | `yahoo_finance` | 입력에서 전달 |
| data_type | string | `prices` | 입력에서 전달 |
| domain | string | `market` | 입력에서 전달 |
| table_name | string | `prices` | 입력에서 전달 |
| symbols | array | `["AAPL", "MSFT"]` | 청크 분할된 심볼 |
| period | string | `1mo` | 입력에서 전달 |
| interval | string | `1d` | 입력에서 전달 |
| file_format | string | `json` | 입력에서 전달 |
| batch_id | string | `c7d3d6c8-...` | 배치 트래커 UUID |
| batch_ds | string | `2025-09-07` | 배치 기준 날짜 |
| batch_total_chunks | integer | `8` | 전체 청크 수 |

## 오류 코드 매핑

| 오류 코드 | 발생 조건 | 재시도 | 운영 액션 |
|-----------|----------|:------:|-----------|
| MISSING_QUEUE_URL | 필수 환경 변수 누락 | ❌ | 배포 설정 확인 |
| INVALID_CHUNK_SIZE | `CHUNK_SIZE` 값 유효성 실패 | ❌ | 환경 변수 조정 |
| SSM_NOT_FOUND | 심볼 파라미터 조회 실패 | ✅ | S3/이벤트 폴백 |
| S3_OBJECT_NOT_FOUND | 심볼 파일 존재하지 않음 | ✅ | 이벤트 payload 사용 |
| SQS_SEND_FAILED | 배치 전송 실패 | ✅ | 재시도 후 DLQ 모니터링 |
| SQS_PARTIAL_FAILURE | 부분 실패 응답 수신 | ✅ | 실패 아이템 재전송 |

## 성능 및 모니터링

| 항목 | 권장 값 | 설명 |
|------|---------|------|
| Batch Size | `SQS_SEND_BATCH_SIZE` (기본 10) | `SendMessageBatch` 호출 크기 |
| Chunk Size | `CHUNK_SIZE` | 심볼 그룹화 (환경별 튜닝) |
| Duration 지표 | 환경별 SLO | CloudWatch 알람 설정 |
| Errors/Throttles | 0 | 발생 시 즉시 대응 |

## 참고 사항

| 항목 | 내용 |
|------|------|
| 심볼 파일 배포 | `data/symbols/*` → 아티팩트 버킷 (`{env}-data-platform-artifacts-{account}`) |
| 스크립트 | `python data/symbols/find_symbols.py`로 최신 심볼 갱신 |
| 환경별 파라미터 | `infrastructure/config/environments/*.py`에서 관리 |
