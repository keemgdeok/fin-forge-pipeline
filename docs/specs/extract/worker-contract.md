# Worker Lambda — I/O 계약 명세

| 항목 | 내용 |
|------|------|
| 책임 | 외부 데이터 공급자에서 가격 데이터를 수집해 RAW S3에 저장하고 배치 상태를 갱신 |
| 코드 기준 | `src/lambda/functions/ingestion_worker/handler.py` |
| 배포 리소스 | `{environment}-daily-prices-data-ingestion-worker` (Python 3.12) |

## 환경 변수

| 변수명 | 필수 | 기본값 | 설명 |
|--------|:---:|--------|------|
| `ENVIRONMENT` | ✅ | - | 배포 환경 식별자 |
| `RAW_BUCKET` | ✅ | - | RAW 데이터가 저장될 S3 버킷 |
| `ENABLE_GZIP` | ❌ | `false` | 업로드 시 GZIP 압축 여부 |
| `BATCH_TRACKING_TABLE` | ❌ | - | DynamoDB 배치 트래커 테이블 이름 |
| `RAW_MANIFEST_BASENAME` | ❌ | `_batch` | 매니페스트 기본 파일명 |
| `RAW_MANIFEST_SUFFIX` | ❌ | `.manifest.json` | 매니페스트 확장자 |

## SQS 메시지 계약

| 필드 | 타입 | 필수 | 제약/설명 | 예시 |
|------|------|:---:|-------------|------|
| `data_source` | string | ✅ | 1–50자 | `yahoo_finance` |
| `data_type` | string | ✅ | 1–30자 | `prices` |
| `domain` | string | ✅ | 1–50자 | `market` |
| `table_name` | string | ✅ | 1–50자 | `prices` |
| `symbols` | array | ✅ | 1–10개 | `["AAPL", "MSFT"]` |
| `period` | string | ❌ | Yahoo 지원값 | `1mo` |
| `interval` | string | ❌ | Yahoo 지원값 | `1d` |
| `file_format` | string | ❌ | `json` 또는 `csv` (`parquet` 요청 시 JSON 저장) | `json` |
| `correlation_id` | string | ❌ | 1–100자 | `batch-001` |
| `batch_id` | string | ✅ | UUID v4 | `c7d3...` |
| `batch_ds` | string | ✅ | `YYYY-MM-DD` | `2025-09-07` |
| `batch_total_chunks` | integer | ✅ | > 0 | `8` |

## 외부 데이터 호출 요약

| 항목 | 값 |
|------|-----|
| 공급자 | Yahoo Finance (`yfinance`) |
| 재시도 전략 | 명시적 재시도 없음 (오류 시 해당 심볼 스킵) |
| 라이브러리 미존재 시 | 빈 리스트 반환 (정상 처리) |
| 전달 파라미터 | 이벤트 payload의 `symbols`, `period`, `interval`을 그대로 사용 |

## RAW S3 저장 규칙

| 항목 | 값/예시 | 설명 |
|------|---------|------|
| 경로 패턴 | `s3://{RAW_BUCKET}/{domain}/{table_name}/interval={interval}/data_source={data_source}/year={YYYY}/month={MM}/day={DD}/` | Hive 파티션 구조 |
| 파일명 | `{symbol}.{ext}[.gz]` | 심볼별 1일치 데이터 저장 |
| 지원 형식 | JSON Lines, CSV | `ENABLE_GZIP=true` 시 `.gz` 확장자 추가 |
| Parquet 요청 | 저장은 JSON으로 수행 후 Transform에서 Parquet 변환 |

## 파일 형식 비교

| 형식 | 확장자 | 압축률 | 호환성 | 쿼리 성능 | 권장 용도 |
|------|--------|:------:|:------:|:---------:|----------|
| JSON Lines | `.json` | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | 기본값 (개발 친화적) |
| CSV | `.csv` | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐ | Excel/수동 분석 |

## 배치 트래커 & 매니페스트 흐름

| 단계 | 설명 | 저장소 |
|------|------|--------|
| 청크 처리 | `processed_chunks` 증가, partition summary 추가 | DynamoDB `BATCH_TRACKING_TABLE` |
| 임시 요약 | `manifests/tmp/{batch_id}/*.json` 파일 기록 | RAW 버킷 |
| 최종 완료 | `_batch.manifest.json` 생성 | RAW 버킷 |
| 후속 실행 | Runner/운영 스크립트가 `manifest_keys`를 구성해 Step Functions 실행 | 외부 스크립트 |

## Partial Batch Failure 응답

| 시나리오 | Lambda 응답 | SQS 동작 |
|----------|-------------|----------|
| 전체 성공 | `{"batchItemFailures": []}` | 모든 메시지 삭제 |
| 부분 실패 | 실패한 `messageId` 목록 포함 | 해당 메시지만 재전달 |
| 전체 실패 | 예외 발생 | 전체 메시지 재전달 (receiveCount 증가) |

## SQS 재시도 구성

| 항목 | 값 | 설명 |
|------|-----|------|
| Event Source Mapping | `report_batch_item_failures=true` | 실패 항목만 재전달 |
| Visibility Timeout | `worker_timeout × 6` (Dev 1800초) | 메시지 재처리 시간 확보 |
| `maxReceiveCount` | `max_retries` (Dev 5) | 초과 시 DLQ 이동 |
| DLQ | `{env}-ingestion-dlq` (14일 보관) | 운영자가 재처리 |

## 성능/운영 파라미터

| 항목 | 관리 위치 | 기본값(Dev) | 비고 |
|------|-----------|--------------|------|
| Worker 메모리 | `worker_memory` | 512MB | 환경별 조정 가능 |
| Worker 타임아웃 | `worker_timeout` | 300초 | 공급자 응답 속도 기반 |
| 예약 동시성 | `worker_reserved_concurrency` | 0 | 0이면 제한 없음 |
| SQS 배치 크기 | `sqs_batch_size` | 1 | 처리량 vs 실패 격리 |
| CloudWatch 알람 | `ingestion_stack.py` | QueueDepth, MessageAge, Errors | Dev 값 기준 |

## 데이터 품질 검사

| 검사 항목 | 조건 | 조치 | 로그 |
|------------|------|------|------|
| 필수 필드 | `symbol`, `timestamp` 존재 여부 | 레코드 스킵 | ERROR |
| 심볼 형식 | `^[A-Z0-9._-]{1,20}$` | 레코드 스킵 | WARNING |
| 타임스탬프 | ISO 8601 UTC | 레코드 스킵 | ERROR |
| 가격/볼륨 | 음수 방지 | 경고 후 계속 | WARNING |
| 중복 레코드 | 동일 `symbol+timestamp` | 최근 레코드 유지 | INFO |

## 참고

| 항목 | 설명 |
|------|------|
| 외부 API 재시도 | 명시적 백오프 없음, 실패 심볼은 건너뜀 |
| 심볼 요약 정리 | `_cleanup_chunk_summaries` 호출로 `manifests/tmp` 정리 |
| 환경 파라미터 | `infrastructure/config/environments/*.py`에서 관리 |

---

| 관련 문서 | 경로 |
|----------|------|
| Raw 스키마 | `docs/specs/extract/raw-data-schema.md` |
| SQS 통합 | `docs/specs/extract/sqs-integration-spec.md` |
| 배치 트래커 | `docs/specs/extract/orchestrator-contract.md` |
