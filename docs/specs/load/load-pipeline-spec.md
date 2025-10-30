# Load Pipeline — Data Contracts

| 항목        | 값                                                                                                             |
| ----------- | -------------------------------------------------------------------------------------------------------------- |
| 목적        | Curated S3 이벤트가 Load Event Publisher Lambda로 전달되는 데이터 구조 정의                                    |
| 코드 기준   | `infrastructure/pipelines/load/load_pipeline_stack.py`, `src/lambda/functions/load_event_publisher/handler.py` |
| 출력 소비자 | 도메인별 Load SQS 큐 (`{env}-{domain}-load-queue`)                                                             |

### S3 EventBridge 이벤트 구조

| 필드                 | 타입    | 필수 | 설명                                   |
| -------------------- | ------- | :--: | -------------------------------------- |
| `source`             | string  |  ✅  | `"aws.s3"`                             |
| `detail-type`        | string  |  ✅  | `"Object Created"`                     |
| `detail.bucket.name` | string  |  ✅  | Curated 버킷 이름                      |
| `detail.object.key`  | string  |  ✅  | Parquet 객체 키                        |
| `detail.object.size` | integer |  ✅  | 바이트 크기 (`>= MIN_FILE_SIZE_BYTES`) |
| `detail.object.etag` | string  |  ❌  | 객체 ETag                              |

### Curated 객체 키 규칙

| 세그먼트      | 예시                | 설명                                                    |
| ------------- | ------------------- | ------------------------------------------------------- |
| `domain`      | `market`            | 도메인                                                  |
| `table`       | `prices`            | 테이블                                                  |
| `interval`    | `interval=1d`       | 파티션 간격                                             |
| `data_source` | `data_source=yahoo` | 데이터 소스(선택)                                       |
| `year`        | `year=2025`         | 4자리 연도                                              |
| `month`       | `month=09`          | 2자리 월                                                |
| `day`         | `day=10`            | 2자리 일                                                |
| `layer`       | `layer=<layer>`     | 허용 레이어는 `adjusted`, `technical_indicator` 두 가지 |
| `object`      | `part-0000.parquet` | Parquet 파일                                            |
