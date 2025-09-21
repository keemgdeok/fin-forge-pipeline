# Orchestrator Lambda — I/O 계약 명세

본 문서는 Extract 파이프라인의 Orchestrator Lambda 함수의 입력/출력 계약을 정의합니다. 모든 예시는 UTC 기준입니다.

## 기본 정보

| 항목 | 값 |
|------|-----|
| **Function Name** | `{environment}-daily-prices-data-orchestrator` |
| **Runtime** | Python 3.12 |
| **Memory** | 256MB |
| **Timeout** | 60초 |
| **Trigger** | EventBridge Rule (Schedule) |

## 입력 명세

| 필드 | 타입 | 필수 | 기본값 | 예시 | 설명 |
|------|------|:---:|--------|------|------|
| **data_source** | string | Y | - | `yahoo_finance` | 데이터 소스 식별자 |
| **data_type** | string | Y | - | `prices` | 데이터 타입 |
| **domain** | string | Y | - | `market` | 도메인 식별자 |
| **table_name** | string | Y | - | `prices` | 대상 테이블명 |
| **symbols** | array | C | `[]` | `["AAPL", "MSFT"]` | 심볼 목록 (빈 경우 외부 로드) |
| **period** | string | N | `1mo` | `1mo` | 조회 기간 |
| **interval** | string | N | `1d` | `1d` | 시간 간격 |
| **file_format** | string | N | `json` | `json` | json\|csv\|parquet |
| **correlation_id** | string | N | 자동생성 | `manual-001` | 수동 실행 추적 ID |
| **dry_run** | boolean | N | `false` | `true` | SQS 전송 없이 로그만 |

## 환경변수

| 변수명 | 필수 | 기본값 | 설명 |
|--------|:---:|--------|------|
| **ENVIRONMENT** | Y | - | 배포 환경 |
| **QUEUE_URL** | Y | - | 대상 SQS 큐 URL |
| **CHUNK_SIZE** | N | `5` | 심볼 그룹화 크기 |
| **SQS_SEND_BATCH_SIZE** | N | `10` | SQS 배치 크기 (1-10) |
| **SYMBOLS_SSM_PARAM** | N | - | SSM 파라미터 경로 |
| **SYMBOLS_S3_BUCKET** | N | - | S3 설정 버킷 |
| **SYMBOLS_S3_KEY** | N | - | S3 심볼 파일 키 |

## 심볼 로딩 전략

심볼 목록은 다음 우선순위로 결정됩니다:

| 순위 | 소스 | 환경변수 | 장점 |
|:---:|------|---------|----- |
| **1** | SSM Parameter | `SYMBOLS_SSM_PARAM` | 중앙 관리, 암호화 |
| **2** | S3 Object | `SYMBOLS_S3_BUCKET` + `SYMBOLS_S3_KEY` | 대용량 지원 |
| **3** | Event 입력 | `event.symbols` | 즉시 사용 |
| **4** | Fallback | 고정값 `["AAPL"]` | 항상 동작 |

## 출력 명세

### 성공 응답

| 필드 | 타입 | 예시 | 설명 |
|------|------|------|------|
| **published** | integer | `24` | 발행된 SQS 메시지 수 |
| **chunks** | integer | `8` | 생성된 청크 수 |
| **environment** | string | `dev` | 실행 환경 |

### SQS 메시지 구조

| 필드 | 타입 | 예시 | 설명 |
|------|------|------|------|
| **data_source** | string | `yahoo_finance` | 입력에서 그대로 전달 |
| **data_type** | string | `prices` | 입력에서 그대로 전달 |
| **domain** | string | `market` | 입력에서 그대로 전달 |
| **table_name** | string | `prices` | 입력에서 그대로 전달 |
| **symbols** | array | `["AAPL", "MSFT"]` | 청크 분할된 심볼 |
| **period** | string | `1mo` | 입력에서 그대로 전달 |
| **interval** | string | `1d` | 입력에서 그대로 전달 |
| **file_format** | string | `json` | 입력에서 그대로 전달 |

## 오류 처리

| 오류 코드 | 원인 | 재시도 | 대응 |
|-----------|------|:-----:|------|
| **MISSING_QUEUE_URL** | 환경변수 누락 | ❌ | 설정 확인 |
| **INVALID_CHUNK_SIZE** | 잘못된 청크 크기 | ❌ | 환경변수 수정 |
| **SSM_NOT_FOUND** | SSM 파라미터 없음 | ✅ | S3 폴백 |
| **S3_OBJECT_NOT_FOUND** | S3 객체 없음 | ✅ | Event 사용 |
| **SQS_SEND_FAILED** | SQS 전송 실패 | ✅ | 재시도 후 DLQ |
| **SQS_PARTIAL_FAILURE** | 일부 메시지 실패 | ✅ | 실패분만 재시도 |

## 성능 설정

| 심볼 수 | 메모리 | 타임아웃 | 청크 크기 | 배치 크기 |
|:-------:|--------|:--------:|:--------:|:--------:|
| **1-50** | 256MB | 30초 | 5 | 10 |
| **50-200** | 512MB | 60초 | 10 | 10 |
| **200-500** | 768MB | 120초 | 20 | 10 |
| **500+** | 1024MB | 180초 | 50 | 10 |

## 모니터링

| 지표 | 임계값 | 알람 조건 |
|------|--------|-----------| 
| **Duration** | 45초 | > 임계값 2회 연속 |
| **Errors** | 0 | ≥ 1 즉시 |
| **Throttles** | 0 | ≥ 1 즉시 |

---

*본 명세는 `src/lambda/functions/ingestion_orchestrator/handler.py` 기반입니다.* CDK 배포 시 기본 `SYMBOLS_S3_*` 값은 `data/symbols/` 디렉터리의 심볼 파일을 아티팩트 버킷(`{env}-data-platform-artifacts-{account}`)으로 업로드하도록 구성되어 있습니다. 최신 NASDAQ/S&P 500 심볼은 `python data/symbols/find_symbols.py` 스크립트로 갱신한 뒤 PR에 포함하면 됩니다.
