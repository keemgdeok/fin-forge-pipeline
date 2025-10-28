# LocalStack E2E Test Guide

| 구분            | 내용                                                                                                               |
| --------------- | ------------------------------------------------------------------------------------------------------------------ |
| **역할**        | LocalStack 환경에서 ingestion → transform → load 파이프라인 통합 검증                                              |
| **필수 서비스** | `s3`, `sqs`, `dynamodb`, `ssm`, `stepfunctions`, `lambda`                                                          |
| **환경 변수**   | `LOCALSTACK_ENDPOINT`, `LOCALSTACK_INTERNAL_ENDPOINT`, `AWS_REGION`/`AWS_DEFAULT_REGION`                           |
| **실행**        | `pytest tests/e2e/`                                                                                                |
| **옵션**        | - `E2E_PIPELINE_LIGHT_MODE=1`: Light 모드 (테스트에서는 SKIP)                                                      |
|                 | - `E2E_ENABLE_FULL_WORKFLOW=1`: 100개 manifest 처리 (default OFF)                                                  |
|                 | - `E2E_FULL_PIPELINE_MANIFESTS`, `E2E_WORKFLOW_LIGHT_MANIFESTS`, `E2E_WORKFLOW_FULL_MANIFESTS`: manifest 개수 조정 |
