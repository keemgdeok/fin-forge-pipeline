<div id="top">

<!-- HEADER STYLE: CLASSIC -->

<div align="center">

# <code>❯ fin-forge-pipeline </code>

<em>Serverless financial data pipelines delivered as code-first products on AWS</em>

<!-- BADGES -->

<em>Built with the tools and technologies:</em>

<img src="https://img.shields.io/badge/AWS%20Step%20Functions-D04E64?style=flat&logo=awsstepfunctions&logoColor=white" alt="AWS Step Functions">
<img src="https://img.shields.io/badge/AWS%20Glue-8C4FFF?style=flat&logo=awsglue&logoColor=white" alt="AWS Glue">
<img src="https://img.shields.io/badge/AWS%20Lambda-FF9900?style=flat&logo=awslambda&logoColor=white" alt="AWS Lambda">
<img src="https://img.shields.io/badge/Amazon%20SQS-FF4F8B?style=flat&logo=amazonsqs&logoColor=white" alt="Amazon SQS">
<br>
<img src="https://img.shields.io/badge/Amazon%20S3-569A31?style=flat&logo=amazons3&logoColor=white" alt="Amazon S3">
<img src="https://img.shields.io/badge/Amazon%20DynamoDB-4053D6?style=flat&logo=amazondynamodb&logoColor=white" alt="Amazon DynamoDB">
<img src="https://img.shields.io/badge/Amazon%20EventBridge-FF4F8B?style=flat&logo=amazoneventbridge&logoColor=white" alt="Amazon EventBridge">
<br>
<img src="https://img.shields.io/badge/AWS%20CDK-1F43F4?style=flat&logo=amazonaws&logoColor=white" alt="AWS CDK">
<img src="https://img.shields.io/badge/LocalStack-4AB5E6?style=flat&logo=localstack&logoColor=white" alt="LocalStack">
<img src="https://img.shields.io/badge/Amazon%20CloudWatch-FF4F00?style=flat&logo=amazoncloudwatch&logoColor=white" alt="Amazon CloudWatch">
<img src="https://img.shields.io/badge/Amazon%20SNS-FF9999?style=flat&logo=amazonaws&logoColor=white" alt="Amazon SNS">
<br>
<img src="https://img.shields.io/badge/JSON-000000.svg?style=default&logo=JSON&logoColor=white" alt="JSON">
<img src="https://img.shields.io/badge/npm-CB3837.svg?style=default&logo=npm&logoColor=white" alt="npm">
<img src="https://img.shields.io/badge/TOML-9C4121.svg?style=default&logo=TOML&logoColor=white" alt="TOML">
<img src="https://img.shields.io/badge/precommit-FAB040.svg?style=default&logo=pre-commit&logoColor=black" alt="precommit">
<img src="https://img.shields.io/badge/Ruff-D7FF64.svg?style=default&logo=Ruff&logoColor=black" alt="Ruff">
<img src="https://img.shields.io/badge/Pytest-0A9EDC.svg?style=default&logo=Pytest&logoColor=white" alt="Pytest">
<br>
<img src="https://img.shields.io/badge/Python-3776AB.svg?style=default&logo=Python&logoColor=white" alt="Python">
<img src="https://img.shields.io/badge/GitHub%20Actions-2088FF.svg?style=default&logo=GitHub-Actions&logoColor=white" alt="GitHub%20Actions">
<img src="https://img.shields.io/badge/pandas-150458.svg?style=default&logo=pandas&logoColor=white" alt="pandas">
<img src="https://img.shields.io/badge/Pydantic-E92063.svg?style=default&logo=Pydantic&logoColor=white" alt="Pydantic">
<img src="https://img.shields.io/badge/YAML-CB171E.svg?style=default&logo=YAML&logoColor=white" alt="YAML">

</div>
<br>

______________________________________________________________________

## Table of Contents

- [❯ fin-forge-pipeline ](#-fin-forge-pipeline-)
  - [Table of Contents](#table-of-contents)
  - [Architecture](#architecture)
    - [End-to-end flow](#end-to-end-flow)
    - [Technical Concerns](#technical-concerns)
  - [Features](#features)
  - [Key Directories](#key-directories)
  - [Quick Start](#quick-start)
    - [Prerequisites](#prerequisites)
    - [Environment setup](#environment-setup)
  - [Common Commands](#common-commands)
    - [Synthesize & deploy](#synthesize--deploy)
    - [Data validation & runbooks](#data-validation--runbooks)
  - [Testing & Quality Gates](#testing--quality-gates)

<br>

______________________________________________________________________

## Architecture

<p align="center">
  <img src="docs/architecture/architecture.svg" alt="Serverless Data Pipeline Architecture" width="100%" />
</p>

### End-to-end flow

[\[**Extract**\]](docs/diagrams/extract/README.md)\
EventBridge → Orchestrator Lambda → Ingestion SQS → Worker Lambda → Raw S3 + DynamoDB Stream

[\[**Transform**\]](docs/diagrams/transform/README.md)\
Manifest 기반 Step Functions → Preflight Lambda → Glue Compaction/ETL/Indicators → Curated S3 + Catalog

[\[**Load**\]](docs/diagrams/load/README.md)\
Curated S3 ObjectCreated → Load Event Publisher Lambda → Load SQS → On-premise Loader(미구현)

\*\*세부 문서 링크

<br>

### [Technical Concerns](https://versed-racer-357.notion.site/technical-concerns-271cd94d4b5e80b484ede79b5e5e5c8d)

\*\*Notion 링크

<br>

______________________________________________________________________

## Features

|     | Component         | Details                                                                                                                                                                                                       |
| :-- | :---------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| ⚙️  | **Architecture**  | <ul><li>AWS CDK 기반 Pipeline-as-a-Product Design</li><li>공유 스택(Security/Storage/Governance)과 도메인 스택 조합</li><li>Lambda + Step Functions + Glue로 구성된 완전 서버리스 데이터 파이프라인</li></ul> |
| 🔩  | **Code Quality**  | <ul><li>Ruff/mypy 으로 정적 분석 & 타입 검증 수행</li><li>pre-commit hook으로 일관된 스타일과 보안 스캔(Bandit) 적용</li></ul>                                                                                |
| 📄  | **Documentation** | <ul><li>`docs/` 에 Architecture/Diagram/Specs 문서 수록</li></ul>                                                                                                                                             |
| 🔌  | **Integrations**  | <ul><li>GitHub Actions + OIDC AssumeRole로 Secretless CI/CD 구현</li><li>CloudWatch Alarms + SNS 연동으로 SFN/Glue 실패 이벤트 통합 모니터링 </li></ul>                                                       |
| 🧩  | **Modularity**    | <ul><li>공유 Construct + 도메인별 Stack으로 인프라 재사용</li><li>Lambda Layer로 공통 로직과 third-party 의존성 분리</li></ul>                                                                                |
| 🧪  | **Testing**       | <ul><li>pytest 기반 단위/통합 테스트 스위트(`tests/`)</li><li>공유 유틸(Manifest/Queue helper)을 통한 데이터 품질 및 큐 상태 검증 지원</li></ul>                                                              |
| ⚡️  | **Performance**   | <ul><li>SQS 팬아웃과 Step Functions Map maxConcurrency로 병렬 처리량 제어</li><li>Glue 5.0, Zstd 압축, Parquet 최적화를 통한 ETL 성능/비용 개선</li></ul>                                                     |
| 🛡️  | **Security**      | <ul><li>SecurityStack에서 IAM 역할/정책을 중앙 관리하고 버킷/잡 단위 최소 권한 적용</li><li>KMS 암호화된 SNS와 GitHub OIDC 신뢰정책으로 CI/CD 경로 강화</li></ul>                                             |
| 📦  | **Dependencies**  | <ul><li>Python: `requirements.txt` 및 Layer별 requirements로 환경 분리</li><li>NPM/CDK: `package.json`, `package-lock.json`으로 IaC 패키지 고정</li></ul>                                                     |
| 🚀  | **Scalability**   | <ul><li>Extract/Transform/Load Stack 모듈화 → 신규 도메인 추가 용이</li><li> S3→SQS Load 파이프라인을 도메인별로 확장 용이</li></ul>                                                                          |

<br>

______________________________________________________________________

## Key Directories

| Path                                  | Purpose                                                         |
| ------------------------------------- | --------------------------------------------------------------- |
| `infrastructure/config/environments/` | 환경별(region, sizing, feature flag) 타입 세이프 설정 모듈      |
| `infrastructure/constructs/`          | Storage/Orchestrator/Security 패턴을 위한 재사용 CDK constructs |
| `infrastructure/core/`                | IAM, 스토리지, 모니터링 기반을 제공하는 공유 스택               |
| `infrastructure/pipelines/`           | 도메인별 ingestion/processing 스택 (도메인당 디렉토리)          |
| `src/lambda/functions/`               | 핵심 파이프라인 Lambda(Orchestrator/Worker/Preflight/Load)      |
| `src/lambda/layers/`                  | 공용/도메인별 Lambda Layer (common, data/\<domain>/, load)      |
| `src/glue/jobs/`                      | RAW→Curated / Glue ETL 스크립트                                 |
| `src/step_functions/`                 | sfn 기반 워크플로 정의                                          |
| `docs/`                               | Architecture/Diagram/Specs/Screenshots 문서                     |
| `scripts/`                            | 배포/검증 스크립트                                              |
| `tests/`                              | 단위/통합/E2E 테스트 스위트 & 공용 fixture                      |

<br>

______________________________________________________________________

## Quick Start

### Prerequisites

- Python 3.12+
- Node.js 20+ and npm
- configured AWS CLI account/region
- AWS CDK toolkit & a bootstrapped environment

### Environment setup

1. **Repository Clone**
   ```bash
   git clone https://github.com/keemgdeok/fin-forge-pipeline.git
   cd fin-forge-pipeline
   ```
1. **Virtual Environment 생성 및 활성화**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate
   ```
1. **Python 의존성 설치**
   ```bash
   pip install -r requirements.txt
   pip install -r src/lambda/layers/common/requirements.txt
   pip install -r src/lambda/layers/data/market/dependencies/requirements.txt  # Domain Third-Party layer
   ```
1. **CDK 의존성 설치**
   ```bash
   npm ci
   npm install -g aws-cdk
   ```
1. **Bootstrap (account/region 최초 1회)**
   ```bash
   cdk bootstrap aws://<account>/<region>
   ```

<br>

______________________________________________________________________

## Common Commands

### Synthesize & deploy

```bash
# CloudFormation 템플릿 생성
cdk synth

# 로컬 변경 사항과 배포된 스택 비교
# environment : dev | staging | prod
cdk diff --context environment=dev

# 지정한 환경으로 전체 스택 배포
cdk deploy '*' --context environment=dev

# Python 배포 스크립트 사용
python scripts/deploy/deploy.py --environment dev
```

### Data validation & runbooks

```bash
# 배포 후 검증 실행
python scripts/validate/validate_pipeline.py --environment dev
```

[ScreenShots](docs/screenshots/README.md)

<br>

______________________________________________________________________

## Testing & Quality Gates

```bash
# 린트 및 포맷팅
ruff check src tests
ruff format src tests

# 정적 타입 검사
mypy src

# pre-commit hook(설치 및 전체 실행)
pip install pre-commit && pre-commit install
pre-commit run --all-files

# 단위/통합/e2e 테스트 실행
pytest tests/unit
pytest tests/integration

# LocalStack 기반 통합/E2E 실행 (사전 Docker 필요)
./scripts/localstack/start_localstack.sh
pytest tests/integration
pytest tests/e2e
./scripts/localstack/stop_localstack.sh

# Spark 통합 테스트 (PySpark 컨테이너 필요)
./scripts/spark/run_tests.sh pytest tests/integration/transform/test_indicators_etl_local_spark.py
# 테스트 후 이미지 제거(선택)
./scripts/spark/clean.sh
```
