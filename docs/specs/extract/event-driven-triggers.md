# Extract Trigger Data Contracts

| 항목      | 내용                                                              |
| --------- | ----------------------------------------------------------------- |
| 목적      | Orchestrator Lambda가 수신하는 트리거 이벤트 페이로드 스키마 정의 |
| 코드 기준 | `infrastructure/pipelines/daily_prices_data/ingestion_stack.py`   |
| Consumer  | `src/lambda/functions/ingestion_orchestrator/handler.py`          |

### 공통 이벤트 페이로드

| 필드             | 타입            | 필수 | 기본값                                                      | 설명                                                                      |
| ---------------- | --------------- | :--: | ----------------------------------------------------------- | ------------------------------------------------------------------------- |
| `data_source`    | string          |  ✅  | `yahoo_finance`                                             | 데이터 공급자 식별자                                                      |
| `data_type`      | string          |  ✅  | `prices`                                                    | 데이터 유형                                                               |
| `domain`         | string          |  ✅  | `market`                                                    | 비즈니스 도메인                                                           |
| `table_name`     | string          |  ✅  | `prices`                                                    | 대상 테이블                                                               |
| `symbols`        | array\[string\] |  ❌  | 환경 설정 `ingestion_symbols`<br>(기본: `["AAPL", "MSFT"]`) | 미제공 시 Orchestrator가 SSM/S3 조회 후 최종적으로 없으면 `["AAPL"]` 사용 |
| `period`         | string          |  ✅  | `1mo`                                                       | 공급자 쿼리 기간                                                          |
| `interval`       | string          |  ✅  | `1d`                                                        | 공급자 쿼리 주기                                                          |
| `file_format`    | string          |  ✅  | `json`                                                      | 워커 저장 포맷 (`json`/`csv`)                                             |
| `trigger_type`   | string          |  ❌  | 호출 패턴 (`schedule`, `manual`)                            |                                                                           |
| `batch_id`       | string          |  ❌  | 없음                                                        | 사전 생성 배치 ID 사용 시 전달                                            |
| `batch_ds`       | string          |  ❌  | 없음                                                        | `YYYY-MM-DD` 형식의 배치 날짜 오버라이드                                  |
| `correlation_id` | string          |  ❌  | 없음                                                        | 로깅용 상관 키                                                            |

### EventBridge Scheduled Trigger

| 항목          | 값                   | 설명                                |
| ------------- | -------------------- | ----------------------------------- |
| `detail-type` | `"Scheduled Event"`  | EventBridge 기본 값                 |
| `detail`      | 공통 이벤트 페이로드 | Orchestrator Lambda에 전달되는 본문 |
| `time`        | ISO 8601             | 실행 시각                           |
| `resources`   | Rule ARN             | Cron 규칙 참조                      |

### Manual Invocation 변형

| 필드           | 허용 값          | 설명                                  |
| -------------- | ---------------- | ------------------------------------- |
| `symbols`      | 사용자 정의 배열 | 제공 시 외부 심볼 소스를 건너뜀       |
| `batch_id`     | UUID 문자열      | 배치 추적 키를 수동 지정              |
| `batch_ds`     | `YYYY-MM-DD`     | 실행 날짜를 특정 일자로 고정          |
| `trigger_type` | `"manual"`       | 스케줄 호출과 구분하기 위해 사용 가능 |
