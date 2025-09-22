# Worker Lambda — I/O 계약 명세

본 문서는 Extract 파이프라인의 Worker Lambda 함수(Ingestion Worker)의 입력/출력 계약을 정의합니다. Worker는 실제 외부 데이터 수집 및 Raw 데이터 저장을 담당합니다.

## 기본 정보

| 항목 | 값 |
|------|-----|
| **Function Name** | `{environment}-daily-prices-data-ingestion-worker` |
| **Runtime** | Python 3.12 |
| **Memory** | 512MB+ (설정 가능) |
| **Timeout** | 300초 (기본, 설정 가능) |
| **Trigger** | SQS Queue Event |

## 환경변수

| 변수명 | 필수 | 기본값 | 설명 |
|--------|:---:|--------|------|
| **ENVIRONMENT** | Y | - | 배포 환경 |
| **RAW_BUCKET** | Y | - | 대상 S3 Raw 버킷 |
| **ENABLE_GZIP** | N | `false` | S3 업로드 시 GZIP 압축 |

## 입력 명세 (SQS 메시지)

| 필드 | 타입 | 필수 | 제약 | 예시 | 설명 |
|------|------|:---:|------|------|------|
| **data_source** | string | Y | 1-50자 | `yahoo_finance` | 데이터 소스 식별자 |
| **data_type** | string | Y | 1-30자 | `prices` | 데이터 타입 |
| **domain** | string | Y | 1-50자 | `market` | 도메인 식별자 |
| **table_name** | string | Y | 1-50자 | `prices` | 대상 테이블명 |
| **symbols** | array | Y | 1-10개 | `["AAPL", "MSFT"]` | 수집할 심볼 목록 |
| **period** | string | N | Yahoo 지원값 | `1mo` | 조회 기간 |
| **interval** | string | N | Yahoo 지원값 | `1d` | 시간 간격 |
| **file_format** | string | N | json\|csv\|parquet | `json` | 출력 파일 형식 |
| **correlation_id** | string | N | 1-100자 | `batch-001` | 추적용 상관관계 ID |

## 데이터 수집 (Yahoo Finance)

| 항목 | 값 | 제한사항 |
|------|-----|----------|
| **지원 심볼** | 미국 주식 (AAPL, MSFT 등) | 국제 주식 일부 지원 |
| **지원 기간** | `1d`, `5d`, `1mo`, `3mo`, `6mo`, `1y`, `2y`, `5y`, `10y`, `ytd`, `max` | API 제공값 |
| **지원 간격** | `1m`~`3mo` (15가지) | 고빈도 데이터는 제한적 |
| **Rate Limit** | ~2000 요청/시간 | 심볼당 제한 |

### API 제한 및 대응

| 제한 유형 | 임계값 | 대응 방법 | 재시도 |
|-----------|--------|----------|-------|
| **Rate Limiting** | 429 에러 | 1-2초 대기 | 1회 |
| **Server Error** | 500/502/503 | 지수 백오프 | 2회 |
| **Timeout** | > 10초 | 즉시 중단 | 1회 |
| **Not Found** | 404 (심볼 없음) | 로그 후 스킵 | 0회 |

## S3 저장 구조

### 파티셔닝 (Hive 스타일)

```
s3://{RAW_BUCKET}/{domain}/{table_name}/
    interval={interval}/
    data_source={data_source}/
    year={YYYY}/
    month={MM}/
    day={DD}/
    {symbol}.{file_format}[.gz]
```

- `interval`: 이벤트나 환경 설정에서 전달한 수집 해상도(예: `1d`, `1m`).
- `data_source`: 원본 데이터 공급자(예: `yahoo_finance`).
- `year/month/day`: 시세 레코드의 UTC 날짜.
- 파일 이름은 심볼을 기준으로 하며, `ENABLE_GZIP=true` 인 경우 `.gz` 확장자가 추가됩니다.

### 파일 형식 비교

| 형식 | 확장자 | 압축률 | 호환성 | 쿼리 성능 | 권장 용도 |
|------|--------|:------:|:------:|:---------:|----------|
| **JSON Lines** | `.json` | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | 기본값, 개발 친화적 |
| **CSV** | `.csv` | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐ | Excel 호환, 수동 분석 |
| **Parquet** | `.parquet` | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | 대용량 분석 (미래) |

### 파일 크기 가이드

| 형식 | 목표 크기 | 최대 크기 | 심볼당 30일 예상 |
|------|:---------:|:---------:|:---------------:|
| **JSON** | 1-10MB | 50MB | ~9MB |
| **CSV** | 1-5MB | 25MB | ~6MB |
| **Parquet** | 10-100MB | 200MB | ~3MB |

### 백필 & 증분 수집 전략

- **대규모 초기 적재(예: 10년)**: 동일 심볼의 레코드를 UTC 기준 일 단위로 나누어 업로드합니다. 각 날짜는 `year=YYYY/month=MM/day=DD` 파티션에 매핑되므로 Athena/Glue 파티션 수가 예측 가능합니다.
- **일일 증분**: 매일 새로 수집된 값은 동일 파티션 경로(같은 interval/data_source/year/month/day)로 업로드하되, 동형 키가 이미 존재하면 idempotency 로직이 덮어쓰지 않고 건너뜁니다.
- **분석 워크로드**: 장기 분석 시에는 원하는 날짜 범위만 스캔하도록 `year/month/day` 필터를 사용하고, 고빈도(interval=1m 등) 데이터는 별도 파티션으로 분리됩니다.

## 출력 명세 (Partial Batch Failure)

Worker Lambda는 SQS의 Partial Batch Failure 패턴을 사용합니다:

| 시나리오 | 응답 구조 | SQS 동작 | 재시도 |
|----------|-----------|----------|-------|
| **전체 성공** | `{"batchItemFailures": []}` | 모든 메시지 삭제 | 없음 |
| **부분 실패** | `{"batchItemFailures": [{"itemIdentifier": "msg-id"}]}` | 실패한 메시지만 재시도 | 있음 |
| **전체 실패** | Lambda 예외 발생 | 모든 메시지 재시도 | 있음 |

## 오류 처리

| 오류 유형 | 재시도 | 백오프 | DLQ 이동 | 예시 |
|-----------|:-----:|--------|:------:|------|
| **JSON_PARSE_ERROR** | ✅ | - | 5회 후 | 잘못된 메시지 형식 |
| **INPUT_VALIDATION** | ✅ | - | 5회 후 | 필수 필드 누락 |
| **API_RATE_LIMIT** | ✅ | 1-2초 | 3회 후 | Yahoo Finance 제한 |
| **API_SERVER_ERROR** | ✅ | 지수형 (1,2,4초) | 3회 후 | 외부 API 장애 |
| **S3_PERMISSION** | ❌ | - | 즉시 | IAM 권한 오류 |
| **S3_NETWORK** | ✅ | 지수형 | 2회 후 | 네트워크 장애 |
| **NO_DATA** | ❌ | - | - | 주말/휴일 (정상) |

### SQS 재시도 설정

| 설정 | 값 | 설명 |
|------|-----|------|
| **Visibility Timeout** | 1800초 (30분) | Worker timeout × 6 |
| **Max Receive Count** | 5회 | DLQ 이동 전 최대 재시도 |
| **Dead Letter Queue** | ✅ | 5회 실패 후 격리 |
| **Batch Size** | 1-10 | 동시 처리할 메시지 수 |

## 성능 설정

| 규모 | 심볼 수 | 메모리 | 타임아웃 | 배치 크기 | 동시성 |
|:----:|:-------:|--------|:--------:|:--------:|:------:|
| **소규모** | < 100 | 512MB | 300초 | 1 | 10 |
| **중규모** | 100-500 | 768MB | 600초 | 5 | 20 |
| **대규모** | 500+ | 1024MB | 900초 | 10 | 50 |

## 모니터링

### 알람 임계값

| 지표 | 임계값 | 알람 조건 | 조치 |
|------|--------|-----------|------|
| **Duration** | 240초 (80%) | > 임계값 5분 연속 | 타임아웃 검토 |
| **Errors** | 0 | ≥ 1 즉시 | 로그 확인 |
| **Throttles** | 0 | ≥ 1 즉시 | 동시성 증가 |

### 비즈니스 지표

| 지표명 | 정상 범위 | 경고 임계값 | 위험 임계값 |
|--------|-----------|-------------|-------------|
| **처리 속도** | 2-10 심볼/초 | < 1 | < 0.5 |
| **성공률** | > 95% | < 90% | < 80% |
| **DLQ 유입률** | 0% | > 1% | > 5% |

## 데이터 품질

### 검증 체크리스트

| 검증 항목 | 조건 | 액션 | 로그 레벨 |
|----------|------|------|----------|
| **심볼 유효성** | 영숫자, 점, 하이픈만 | 스킵 | WARNING |
| **타임스탬프** | ISO 8601 형식 | 스킵 | ERROR |
| **가격 범위** | ≥ 0 | 경고 후 계속 | WARNING |
| **볼륨 범위** | ≥ 0 | 경고 후 계속 | WARNING |
| **NULL 값 패턴** | 연속 NULL > 5개 | 경고 후 계속 | WARNING |

---

*본 명세는 `src/lambda/functions/ingestion_worker/handler.py`와 `src/lambda/layers/common/python/shared/ingestion/service.py` 구현을 기반으로 작성되었습니다.*
