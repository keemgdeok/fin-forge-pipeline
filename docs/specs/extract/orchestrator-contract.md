# Orchestrator Lambda — I/O 계약 명세

본 문서는 Extract 파이프라인의 Orchestrator Lambda 함수의 입력/출력 계약, 실패 시나리오, 재시도 정책을 정의합니다. 모든 예시는 UTC 기준이며, CDK가 생성한 리소스와 일관되게 유지해야 합니다.

## 📖 목차
- [함수 개요](#함수-개요)
- [입력 명세](#입력-명세)  
- [환경변수 설정](#환경변수-설정)
- [심볼 유니버스 전략](#심볼-유니버스-전략)
- [출력 명세](#출력-명세)
- [오류 처리](#오류-처리)
- [성능 최적화](#성능-최적화)

## 함수 개요

| 항목 | 값 | 설명 |
|------|-----|------|
| **Function Name** | `{environment}-customer-data-orchestrator` | 환경별 함수명 |
| **Runtime** | Python 3.12 | 실행 환경 |
| **Memory** | 256MB (기본) | 메모리 할당 |
| **Timeout** | 60초 (기본) | 최대 실행 시간 |
| **Trigger** | EventBridge Rule (Schedule) | 주 트리거 방식 |
| **Concurrency** | 미제한 | 동시 실행 제한 |

## 입력 명세

### EventBridge Schedule Event (기본 모드)

| 필드 | 타입 | 필수 | 기본값 | 예시 | 제약사항 | 설명 |
|------|------|:---:|--------|------|----------|------|
| **data_source** | string | ✅ | - | `yahoo_finance` | 1-50자 | 데이터 소스 식별자 |
| **data_type** | string | ✅ | - | `prices` | 1-30자 | 데이터 타입 |
| **domain** | string | ✅ | - | `market` | 1-50자 | 도메인 식별자 |
| **table_name** | string | ✅ | - | `prices` | 1-50자 | 대상 테이블명 |
| **symbols** | array | ❓ | `[]` | `["AAPL", "MSFT"]` | 1-100개 | 심볼 목록 (빈 경우 외부 로드) |
| **period** | string | ❌ | `1mo` | `1mo` | Yahoo 지원값 | 조회 기간 |
| **interval** | string | ❌ | `1d` | `1d` | Yahoo 지원값 | 시간 간격 |
| **file_format** | string | ❌ | `json` | `json` | json\|csv\|parquet | 출력 파일 형식 |

### Manual Invocation Event (수동 모드)

| 필드 | 타입 | 필수 | 기본값 | 예시 | 설명 |
|------|------|:---:|--------|------|------|
| **correlation_id** | string | ❌ | 자동생성 | `manual-20250909-001` | 수동 실행 추적 ID |
| **force_symbols** | boolean | ❌ | `false` | `true` | 외부 소스 무시 여부 |
| **dry_run** | boolean | ❌ | `false` | `true` | SQS 전송 없이 로그만 출력 |

### Yahoo Finance 지원값 참조표

| 구분 | 지원값 | 설명 |
|------|--------|------|
| **Period** | `1d`, `5d`, `1mo`, `3mo`, `6mo`, `1y`, `2y`, `5y`, `10y`, `ytd`, `max` | 조회 기간 |
| **Interval** | `1m`, `2m`, `5m`, `15m`, `30m`, `60m`, `90m`, `1h`, `1d`, `5d`, `1wk`, `1mo`, `3mo` | 시간 간격 |

## 환경변수 설정

| 변수명 | 필수 | 타입 | 기본값 | 예시 | 설명 |
|--------|:---:|------|--------|------|------|
| **ENVIRONMENT** | ✅ | string | - | `dev` | 배포 환경 |
| **QUEUE_URL** | ✅ | string | - | `https://sqs.us-east-1.amazonaws.com/.../queue` | 대상 SQS 큐 URL |
| **CHUNK_SIZE** | ❌ | integer | `5` | `5` | 심볼 그룹화 크기 |
| **SQS_SEND_BATCH_SIZE** | ❌ | integer | `10` | `10` | SQS 배치 크기 (1-10) |
| **SYMBOLS_SSM_PARAM** | ❌ | string | - | `/ingestion/symbols` | SSM 파라미터 경로 |
| **SYMBOLS_S3_BUCKET** | ❌ | string | - | `config-bucket` | S3 설정 버킷 |
| **SYMBOLS_S3_KEY** | ❌ | string | - | `symbols/universe.json` | S3 심볼 파일 키 |

## 심볼 유니버스 전략

심볼 목록은 다음 우선순위로 결정됩니다:

| 순위 | 소스 | 환경변수 | 형식 | 장점 | 단점 |
|:---:|------|---------|------|------|------|
| **1** | SSM Parameter | `SYMBOLS_SSM_PARAM` | JSON Array / 줄바꿈 텍스트 | 중앙 관리, 암호화 | SSM 의존성 |
| **2** | S3 Object | `SYMBOLS_S3_BUCKET` + `SYMBOLS_S3_KEY` | JSON Array / 줄바꿈 텍스트 | 대용량 지원 | S3 접근 권한 필요 |
| **3** | Event 입력 | `event.symbols` | JSON Array | 즉시 사용 | 하드코딩 위험 |
| **4** | Fallback | 고정값 | `["AAPL"]` | 항상 동작 | 테스트용만 |

### 지원 데이터 형식

**JSON Array 형식**:
```json
["AAPL", "MSFT", "GOOG", "AMZN", "TSLA"]
```

**줄바꿈 분리 텍스트**:
```
AAPL
MSFT
GOOG
AMZN
TSLA
```

## 출력 명세

### 성공 응답 구조

| 필드 | 타입 | 예시 | 설명 |
|------|------|------|------|
| **published** | integer | `24` | 성공 발행된 SQS 메시지 수 |
| **chunks** | integer | `8` | 생성된 청크(그룹) 수 |
| **environment** | string | `dev` | 실행 환경 |

### SQS 메시지 형식

Orchestrator가 생성하는 각 SQS 메시지는 다음 구조를 가집니다:

| 필드 | 타입 | 전달방식 | 예시 | 설명 |
|------|------|---------|------|------|
| **data_source** | string | 원본 전달 | `yahoo_finance` | 입력에서 그대로 전달 |
| **data_type** | string | 원본 전달 | `prices` | 입력에서 그대로 전달 |
| **domain** | string | 원본 전달 | `market` | 입력에서 그대로 전달 |
| **table_name** | string | 원본 전달 | `prices` | 입력에서 그대로 전달 |
| **symbols** | array | 청크 분할 | `["AAPL", "MSFT", "GOOG"]` | 전체에서 부분집합 추출 |
| **period** | string | 원본 전달 | `1mo` | 입력에서 그대로 전달 |
| **interval** | string | 원본 전달 | `1d` | 입력에서 그대로 전달 |
| **file_format** | string | 원본 전달 | `json` | 입력에서 그대로 전달 |

## 오류 처리

### 입력 검증 오류 매트릭스

| 오류 코드 | 원인 | HTTP Status | 재시도 | 즉시알람 | 조치 방법 |
|-----------|------|-------------|:-----:|:------:|----------|
| **MISSING_QUEUE_URL** | QUEUE_URL 환경변수 누락 | 500 | ❌ | ✅ | 환경변수 설정 확인 |
| **INVALID_CHUNK_SIZE** | CHUNK_SIZE < 1 | 400 | ❌ | ❌ | 환경변수 수정 |
| **INVALID_BATCH_SIZE** | BATCH_SIZE 범위(1-10) 초과 | 400 | ❌ | ❌ | 환경변수 수정 |

### 외부 서비스 오류 매트릭스

| 오류 코드 | 원인 | 재시도 | 백오프 | 폴백 | 최대시도 | DLQ이동 |
|-----------|------|:-----:|--------|------|:------:|:-------:|
| **SSM_PARAMETER_NOT_FOUND** | SSM 파라미터 미존재 | ✅ | 즉시 | S3 로드 | 1회 | ❌ |
| **S3_OBJECT_NOT_FOUND** | S3 객체 미존재 | ✅ | 즉시 | Event 사용 | 1회 | ❌ |
| **SQS_SEND_FAILED** | SQS 전송 실패 | ✅ | 1초 | - | 1회 | ✅ |
| **SQS_PARTIAL_FAILURE** | 일부 메시지 실패 | ✅ | 즉시 | 실패분만 재시도 | 1회 | ✅ |

### 복구 우선순위

| 우선순위 | 시나리오 | 대응시간 | 대응방법 | 담당자 |
|:--------:|----------|:--------:|----------|--------|
| **P0** | SQS 전체 실패 | 5분 | 수동 재실행 | 온콜 엔지니어 |
| **P1** | 심볼 소스 전체 실패 | 30분 | 폴백 설정 활성화 | 개발팀 |
| **P2** | 일부 심볼 처리 실패 | 2시간 | 다음 스케줄 대기 | 모니터링 |

## 성능 최적화

### 규모별 권장 설정

| 심볼 수 | 메모리 | 타임아웃 | 청크 크기 | 배치 크기 | 예상 실행시간 | 비용/월 |
|:-------:|--------|:--------:|:--------:|:--------:|:-------------:|:-------:|
| **1-50** | 256MB | 30초 | 5 | 10 | < 5초 | $2 |
| **50-200** | 512MB | 60초 | 10 | 10 | < 15초 | $8 |
| **200-500** | 768MB | 120초 | 20 | 10 | < 45초 | $25 |
| **500+** | 1024MB | 180초 | 50 | 10 | < 120초 | $60 |

### 배치 처리 최적화 가이드

| 항목 | 권장값 | 근거 | 모니터링 지표 |
|------|--------|------|---------------|
| **청킹 전략** | 심볼 수 / 10 | 병렬 처리 최적화 | Chunks per execution |
| **배치 전송** | 최대 10개 | SQS API 제한 | Messages per batch |
| **메모리 오버헤드** | 심볼당 5MB | JSON 직렬화 고려 | Memory utilization |
| **타임아웃 마진** | 처리시간 × 3 | 네트워크 지연 고려 | Duration vs timeout |

## 모니터링 및 로깅

### CloudWatch 지표 설정

| 지표 | 네임스페이스 | 단위 | 임계값 | 알람조건 | 알람지연 |
|------|--------------|------|--------|----------|----------|
| **Invocations** | AWS/Lambda | Count | - | - | - |
| **Duration** | AWS/Lambda | Milliseconds | 45,000 | > 임계값 | 2회 연속 |
| **Errors** | AWS/Lambda | Count | 0 | ≥ 1 | 즉시 |
| **Throttles** | AWS/Lambda | Count | 0 | ≥ 1 | 즉시 |

### 커스텀 비즈니스 지표

| 지표명 | 설명 | 단위 | 정상 범위 | 경고 임계값 |
|--------|------|------|-----------|-------------|
| **PublishedMessages** | 발행된 SQS 메시지 수 | Count | 10-100 | < 5 또는 > 200 |
| **ProcessedSymbols** | 처리된 총 심볼 수 | Count | 50-500 | < 10 또는 > 1000 |
| **SymbolSource** | 심볼 로딩 소스별 분포 | Percent | SSM 80%+ | Event 50%+ |
| **ChunkingEfficiency** | 청킹 효율성 | Ratio | 0.8-1.0 | < 0.5 |

---

*본 명세는 `src/lambda/functions/ingestion_orchestrator/handler.py` 구현을 기반으로 작성되었습니다.*