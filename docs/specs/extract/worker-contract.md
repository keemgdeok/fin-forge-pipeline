# Worker Lambda — I/O 계약 명세

본 문서는 Extract 파이프라인의 Worker Lambda 함수(Ingestion Worker)의 입력/출력 계약, SQS 이벤트 처리, 그리고 S3 저장 명세를 정의합니다. Worker는 실제 외부 데이터 수집 및 Raw 데이터 저장을 담당합니다.

## 📖 목차
- [함수 개요](#함수-개요)
- [입력 명세](#입력-명세)
- [데이터 수집](#데이터-수집)
- [S3 저장 구조](#s3-저장-구조)
- [출력 명세](#출력-명세)
- [오류 처리](#오류-처리)
- [성능 최적화](#성능-최적화)
- [모니터링](#모니터링)

## 함수 개요

| 항목 | 값 | 설명 |
|------|-----|------|
| **Function Name** | `{environment}-customer-data-ingestion-worker` | 환경별 함수명 |
| **Runtime** | Python 3.12 | 실행 환경 |
| **Memory** | 512MB+ (설정 가능) | 메모리 할당 |
| **Timeout** | 300초 (기본, 설정 가능) | 최대 실행 시간 |
| **Trigger** | SQS Queue Event | 트리거 방식 |
| **Concurrency** | Reserved 설정 가능 | 동시 실행 제어 |

### 환경변수 설정

| 변수명 | 필수 | 타입 | 기본값 | 예시 | 설명 |
|--------|:---:|------|--------|------|------|
| **ENVIRONMENT** | ✅ | string | - | `dev` | 배포 환경 |
| **RAW_BUCKET** | ✅ | string | - | `data-pipeline-raw-dev-1234` | 대상 S3 Raw 버킷 |
| **ENABLE_GZIP** | ❌ | boolean | `false` | `true` | S3 업로드 시 GZIP 압축 |

## 입력 명세

### SQS 메시지 스키마

각 SQS 메시지의 body는 JSON 문자열로, 다음 구조를 가집니다:

| 필드 | 타입 | 필수 | 제약 | 예시 | 설명 |
|------|------|:---:|------|------|------|
| **data_source** | string | ✅ | 1-50자 | `yahoo_finance` | 데이터 소스 식별자 |
| **data_type** | string | ✅ | 1-30자 | `prices` | 데이터 타입 |
| **domain** | string | ✅ | 1-50자 | `market` | 도메인 식별자 |
| **table_name** | string | ✅ | 1-50자 | `prices` | 대상 테이블명 |
| **symbols** | array | ✅ | 1-10개 | `["AAPL", "MSFT"]` | 수집할 심볼 목록 |
| **period** | string | ❌ | Yahoo 지원값 | `1mo` | 조회 기간 |
| **interval** | string | ❌ | Yahoo 지원값 | `1d` | 시간 간격 |
| **file_format** | string | ❌ | json\|csv\|parquet | `json` | 출력 파일 형식 |
| **correlation_id** | string | ❌ | 1-100자 | `batch-001` | 추적용 상관관계 ID |

### SQS 이벤트 구조

| 필드 | 경로 | 타입 | 설명 |
|------|------|------|------|
| **Records** | `event.Records` | array | SQS 메시지 배열 |
| **Message ID** | `record.messageId` | string | 메시지 고유 ID |
| **Receipt Handle** | `record.receiptHandle` | string | 삭제용 핸들 |
| **Body** | `record.body` | string | JSON 직렬화된 페이로드 |
| **Attributes** | `record.attributes` | object | SQS 메타데이터 |

## 데이터 수집

### 지원하는 데이터 소스

#### Yahoo Finance (`yahoo_finance`)

| 항목 | 값 | 제한사항 |
|------|-----|----------|
| **지원 심볼** | 미국 주식 (AAPL, MSFT 등) | 국제 주식 일부 지원 |
| **지원 기간** | `1d`, `5d`, `1mo`, `3mo`, `6mo`, `1y`, `2y`, `5y`, `10y`, `ytd`, `max` | API 제공값 |
| **지원 간격** | `1m`~`3mo` (15가지) | 고빈도 데이터는 제한적 |
| **Rate Limit** | ~2000 요청/시간 | 심볼당 제한 |
| **응답 시간** | 1-5초 | 기간에 따라 변동 |

### API 제한 및 대응

| 제한 유형 | 임계값 | 대응 방법 | 재시도 |
|-----------|--------|----------|-------|
| **Rate Limiting** | 429 에러 | 1-2초 대기 | 1회 |
| **Server Error** | 500/502/503 | 지수 백오프 | 2회 |
| **Timeout** | > 10초 | 즉시 중단 | 1회 |
| **Not Found** | 404 (심볼 없음) | 로그 후 스킵 | 0회 |

## S3 저장 구조

### 파티셔닝 전략 (Hive 스타일)

```
s3://{RAW_BUCKET}/{domain}/{table_name}/ingestion_date={YYYY-MM-DD}/
    data_source={source}/symbol={symbol}/interval={interval}/period={period}/
```

### 파티션 레벨 분석

| 레벨 | 파티션 키 | 카디널리티 | 스캔 효율성 | 예시 |
|:---:|----------|:----------:|:-----------:|------|
| **1** | domain | 낮음 (10개) | ⭐⭐⭐ | `market` |
| **2** | table_name | 낮음 (50개) | ⭐⭐⭐ | `prices` |
| **3** | ingestion_date | 높음 (일별) | ⭐⭐⭐⭐⭐ | `2025-09-09` |
| **4** | data_source | 낮음 (5개) | ⭐⭐⭐ | `yahoo_finance` |
| **5** | symbol | 매우 높음 (1000+) | ⭐⭐⭐⭐ | `AAPL` |
| **6** | interval | 낮음 (15개) | ⭐⭐ | `1d` |
| **7** | period | 낮음 (10개) | ⭐ | `1mo` |

### 파일 형식 비교

| 형식 | 확장자 | Content-Type | 압축률 | 호환성 | 처리 속도 | 권장 용도 |
|------|--------|--------------|:------:|:------:|:---------:|----------|
| **JSON Lines** | `.json` | `application/json` | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | 기본값 |
| **CSV** | `.csv` | `text/csv` | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | Excel 연동 |
| **Parquet** | `.parquet` | `application/octet-stream` | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | 대용량 분석 (미래) |

### 파일명 규칙

| 구성 요소 | 형식 | 예시 | 설명 |
|----------|------|------|------|
| **Timestamp** | `YYYY-MM-DDTHH-MM-SSZ` | `2025-09-09T14-30-45Z` | UTC 기준 ISO 8601 |
| **Extension** | `.{format}` | `.json` | 파일 형식별 확장자 |

### 파일 크기 가이드

| 형식 | 목표 크기 | 최대 크기 | 심볼당 예상 크기 | 30일 데이터 예상 |
|------|-----------|-----------|:---------------:|:-------------:|
| **JSON** | 1-10MB | 50MB | ~300KB | ~9MB |
| **CSV** | 1-5MB | 25MB | ~200KB | ~6MB |
| **Parquet** | 10-100MB | 200MB | ~100KB | ~3MB |

## 출력 명세

### Partial Batch Failure 응답

Worker Lambda는 SQS의 Partial Batch Failure 패턴을 사용합니다:

| 시나리오 | 응답 구조 | SQS 동작 | 재시도 |
|----------|-----------|----------|-------|
| **전체 성공** | `{"batchItemFailures": []}` | 모든 메시지 삭제 | 없음 |
| **부분 실패** | `{"batchItemFailures": [{"itemIdentifier": "msg-id"}]}` | 실패한 메시지만 재시도 | 있음 |
| **전체 실패** | Lambda 예외 발생 | 모든 메시지 재시도 | 있음 |

### 처리 결과 분류

| 결과 | 조건 | 반환값 | 메시지 처리 |
|------|------|--------|------------|
| **성공** | 데이터 수집 + S3 업로드 성공 | 성공 목록에서 제외 | SQS 삭제 |
| **재시도 가능 실패** | 네트워크 오류, API 429 | 실패 목록에 추가 | SQS 재시도 |
| **영구 실패** | JSON 파싱 오류 | 실패 목록에 추가 | 최종적으로 DLQ |
| **데이터 없음** | 주말/휴일 (정상) | 성공으로 처리 | SQS 삭제 |

## 오류 처리

### 메시지별 오류 처리 매트릭스

| 오류 유형 | HTTP | 재시도 | 백오프 | DLQ 이동 | 알람 | 예시 |
|-----------|:---:|:-----:|--------|:------:|:----:|------|
| **JSON_PARSE_ERROR** | 400 | ✅ | - | 5회 후 | ❌ | 잘못된 메시지 형식 |
| **INPUT_VALIDATION** | 400 | ✅ | - | 5회 후 | ❌ | 필수 필드 누락 |
| **API_RATE_LIMIT** | 429 | ✅ | 1-2초 | 3회 후 | ⚠️ | Yahoo Finance 제한 |
| **API_SERVER_ERROR** | 500 | ✅ | 지수형 (1,2,4초) | 3회 후 | ⚠️ | 외부 API 장애 |
| **S3_PERMISSION** | 403 | ❌ | - | 즉시 | 🚨 | IAM 권한 오류 |
| **S3_NETWORK** | 500 | ✅ | 지수형 | 2회 후 | ⚠️ | 네트워크 장애 |
| **NO_DATA** | 200 | ❌ | - | - | ❌ | 주말/휴일 (정상) |

### SQS 재시도 설정

| 설정 | 값 | 설명 |
|------|-----|------|
| **Visibility Timeout** | 1800초 (30분) | Worker timeout × 6 |
| **Max Receive Count** | 5회 | DLQ 이동 전 최대 재시도 |
| **Dead Letter Queue** | ✅ | 5회 실패 후 격리 |
| **Batch Size** | 1-10 | 동시 처리할 메시지 수 |

## 성능 최적화

### 규모별 권장 설정

| 규모 | 심볼 수 | 메모리 | 타임아웃 | 배치 크기 | 동시성 | 예상 처리 시간 | 월 비용 |
|:----:|:-------:|--------|:--------:|:--------:|:------:|:-------------:|:-------:|
| **소규모** | < 100 | 512MB | 300초 | 1 | 10 | < 30초 | $15 |
| **중규모** | 100-500 | 768MB | 600초 | 5 | 20 | < 90초 | $45 |
| **대규모** | 500+ | 1024MB | 900초 | 10 | 50 | < 180초 | $120 |

### 배치 처리 최적화

| 항목 | 권장 설정 | 이유 | 모니터링 지표 |
|------|----------|------|--------------|
| **SQS Batch Size** | 심볼 수에 따라 1-10 | 병렬 처리 vs 오류 격리 | Messages per invocation |
| **Reserved Concurrency** | 예상 부하의 2배 | 콜드 스타트 최소화 | Concurrent executions |
| **Memory Allocation** | API 응답 크기 × 5 | JSON 파싱 메모리 고려 | Memory utilization |
| **Timeout Buffer** | 실제 처리 시간 × 3 | 네트워크 지연 마진 | Duration vs timeout |

## 모니터링

### CloudWatch 표준 지표

| 지표 | 임계값 | 알람 조건 | 알람 지연 | 조치 |
|------|--------|-----------|----------|------|
| **Invocations** | - | - | - | 사용량 추적 |
| **Duration** | 240초 (80%) | > 임계값 | 5분 연속 | 타임아웃 검토 |
| **Errors** | 0 | ≥ 1 | 즉시 | 로그 확인 |
| **Throttles** | 0 | ≥ 1 | 즉시 | 동시성 증가 |

### 비즈니스 지표

| 지표명 | 설명 | 단위 | 정상 범위 | 경고 임계값 | 위험 임계값 |
|--------|------|:----:|-----------|-------------|-------------|
| **처리 속도** | 심볼/초 | Count/Second | 2-10 | < 1 | < 0.5 |
| **성공률** | 성공 메시지 비율 | Percent | > 95% | < 90% | < 80% |
| **S3 업로드 크기** | 파일당 평균 크기 | Bytes | 100KB-1MB | < 50KB 또는 > 5MB | < 10KB 또는 > 10MB |
| **API 응답 시간** | 외부 API 평균 응답 시간 | Milliseconds | 1-3초 | > 5초 | > 10초 |
| **DLQ 유입률** | DLQ 메시지 비율 | Percent | 0% | > 1% | > 5% |

### 알람 우선순위 매트릭스

| 우선순위 | 조건 | 대응 시간 | 대응 방법 | 담당자 |
|:--------:|------|:--------:|----------|--------|
| **P0** | Error Rate > 50% | 15분 | 즉시 조사 및 수정 | 온콜 엔지니어 |
| **P1** | DLQ Messages > 100개 | 1시간 | 근본 원인 분석 | 개발팀 |
| **P2** | Duration > 80% | 4시간 | 성능 최적화 검토 | 개발팀 |
| **P3** | Success Rate < 95% | 24시간 | 트렌드 분석 | 모니터링팀 |

## 데이터 품질

### 검증 체크리스트

| 검증 항목 | 조건 | 액션 | 로그 레벨 |
|----------|------|------|----------|
| **심볼 유효성** | 영숫자, 점, 하이픈만 | 스킵 | WARNING |
| **타임스탬프** | ISO 8601 형식 | 스킵 | ERROR |
| **가격 범위** | ≥ 0 | 경고 후 계속 | WARNING |
| **볼륨 범위** | ≥ 0 | 경고 후 계속 | WARNING |
| **NULL 값 패턴** | 연속 NULL > 5개 | 경고 후 계속 | WARNING |

### S3 메타데이터 태그

모든 업로드 파일에 자동으로 추가되는 태그:

| 태그 키 | 예시 값 | 설명 |
|---------|---------|------|
| `ingestion_timestamp` | `2025-09-09T14:30:45Z` | 수집 실행 시각 |
| `symbol` | `AAPL` | 대표 심볼 (단일 심볼인 경우) |
| `symbol_count` | `3` | 파일 내 심볼 수 |
| `record_count` | `90` | 총 레코드 수 |
| `file_format` | `json` | 파일 형식 |
| `environment` | `dev` | 실행 환경 |

---

*본 명세는 `src/lambda/functions/ingestion_worker/handler.py`와 `src/lambda/layers/common/python/shared/ingestion/service.py` 구현을 기반으로 작성되었습니다.*