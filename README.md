<div id="top">

<!-- HEADER STYLE: CLASSIC -->
<div align="center">

# <code>❯ fin-forge-pipeline </code>

<em>Serverless financial data pipelines delivered as code-first products on AWS.</em>

<!-- BADGES -->
<!-- local repository, no metadata badges. -->

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

---

## Table of Contents

- [Architecture](#architecture)
  - [End-to-end flow](#end-to-end-flow)
- [Features](#features)
- [Key Directories](#key-directories)
- [Quick Start](#quick-start)
  - [Prerequisites](#prerequisites)
  - [Environment setup](#environment-setup)
- [Day-to-day Commands](#day-to-day-commands)
  - [Synthesize & deploy](#synthesize--deploy)
  - [Data validation & runbooks](#data-validation--runbooks)
- [Testing & Quality Gates](#testing--quality-gates)


<br>

---

## Architecture
<p align="center">
  <img src="docs/architecture/architecture.svg" alt="Serverless Data Pipeline Architecture" width="100%" />
</p>

### End-to-end flow
[[**Extract**]](docs/diagrams/extract/README.md)  
EventBridge → Orchestrator Lambda → SQS → Worker Lambda → Raw S3

[[**Transform**]](docs/diagrams/transform/README.md)  
Manifest 기반 Step Functions → Glue Compaction/ETL/Indicators → Curated S3 + Catalog

[[**Load**]](docs/diagrams/load/README.md)  
Curated S3 ObjectCreated → Publisher Lambda → Load SQS → 온프레미스 Loader

**세부 문서 링크 확인


<br>

---


## Features
|      | Component       | Details                              |
| :--- | :-------------- | :----------------------------------- |
| ⚙️  | **Architecture**  | <ul><li>AWS CDK 기반 Pipeline-as-a-Product 설계</li><li>공유 스택(Security/Storage/Governance)과 도메인 스택 조합</li><li>Lambda + Step Functions + Glue로 구성된 완전 서버리스 데이터 파이프라인</li></ul> |
| 🔩 | **Code Quality**  | <ul><li>Ruff/mypy 조합으로 정적 분석과 타입 검증 수행</li><li>pre-commit hook으로 일관된 스타일과 보안 스캔(Bandit) 적용</li></ul> |
| 📄 | **Documentation** | <ul><li>`docs/` 디렉터리에 아키텍처/보안/배포 문서를 구분 수록</li><li>`scripts/validate/validate_pipeline.py`로 배포 후 검증 자동화</li></ul> |
| 🔌 | **Integrations**  | <ul><li>GitHub Actions + OIDC AssumeRole로 시크릿리스 CI/CD 구현</li><li>Step Functions ↔ Glue ↔ SNS 연동으로 워크플로 상태와 알림을 통합 관리</li></ul> |
| 🧩 | **Modularity**    | <ul><li>공유 Construct + 도메인별 Stack으로 인프라 재사용</li><li>Lambda Layer로 공통 로직과 third-party 의존성 분리</li></ul> |
| 🧪 | **Testing**       | <ul><li>pytest 기반 단위·통합 테스트 스위트(tests/)</li><li>공유 유틸(Manifest/Queue helper)을 통한 데이터 품질 및 큐 상태 검증 지원</li></ul> |
| ⚡️  | **Performance**   | <ul><li>SQS 팬아웃과 Step Functions Map maxConcurrency로 병렬 처리량 제어</li><li>Glue 5.0, Zstd 압축, Parquet 최적화를 통한 ETL 성능/비용 개선</li></ul> |
| 🛡️ | **Security**      | <ul><li>SecurityStack에서 IAM 역할·정책을 중앙 관리하고 버킷/잡 단위 최소 권한 적용</li><li>KMS 암호화된 SNS와 GitHub OIDC 신뢰정책으로 CI/CD 경로 강화</li></ul> |
| 📦 | **Dependencies**  | <ul><li>Python: `requirements.txt` 및 Layer별 requirements로 환경 분리</li><li>NPM/CDK: `package.json`, `package-lock.json`으로 IaC 패키지 고정</li></ul> |
| 🚀 | **Scalability**   | <ul><li>`processing_triggers`·`load_domain_configs` 설정으로 신규 도메인 확장 용이</li><li>EventBridge 스케줄/패턴 기반으로 데이터량 증가 시 자동 스케일 대응</li></ul> |

<br>

---


## Key Directories
| Path | Purpose |
| --- | --- |
| `infrastructure/config/environments/` | 환경별(region, sizing, feature flag) 타입 세이프 설정 모듈 |
| `infrastructure/constructs/` | Storage/Orchestrator/Security 패턴을 위한 재사용 CDK constructs |
| `infrastructure/core/` | IAM, 스토리지, 모니터링 기반을 제공하는 공유 스택 |
| `infrastructure/pipelines/` | 도메인별 ingestion/processing 스택 (도메인당 디렉토리) |
| `src/lambda/shared/layers/` | 로깅, 검증, 외부 의존성을 위한 공용 Lambda layer |
| `src/step_functions/` | `aws_cdk.aws_stepfunctions` 기반 워크플로 정의 |
| `docs/` | Architecture/Deploy/Specs 문서 및 다이어그램 |
| `scripts/` | 배포/검증 스크립트 (`deploy/deploy.py`, `validate/validate_pipeline.py`) |
| `tests/` | 단위/통합/E2E/성능 테스트 스위트 & 공용 fixture |


<br>

---
## Quick Start
### Prerequisites
- Python 3.12+
- Node.js 20+ and npm
- AWS CLI configured for the target account/region
- AWS CDK toolkit (`npm install -g aws-cdk`) and a bootstrapped environment (`cdk bootstrap`)

### Environment setup
1. **Clone the repository**
   ```bash
   git clone https://github.com/<org>/fin-forge-pipeline.git
   cd fin-forge-pipeline
   ```
2. **Create and activate a virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate
   ```
3. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   pip install -r src/lambda/layers/common/requirements.txt
   pip install -r src/lambda/layers/market_data_deps/requirements.txt
   pip install -r src/lambda/functions/data_ingestion/requirements.txt
   ```
4. **Install CDK dependencies**
   ```bash
   npm ci
   npm install -g aws-cdk
   ```
5. **Bootstrap (first time per account/region)**
   ```bash
   cdk bootstrap aws://<account>/<region>
   ```

<br>

---

## Day-to-day Commands
### Synthesize & deploy
```bash
# Synthesize CloudFormation templates
cdk synth

# Compare local changes with deployed stacks
cdk diff --context environment=dev

# Deploy all stacks to the chosen environment
cdk deploy '*' --context environment=dev

# Alternative Python deploy script
python scripts/deploy/deploy.py --environment dev
```

### Data validation & runbooks
```bash
# Post-deployment validation
python scripts/validate/validate_pipeline.py --environment dev
```

<br>

---


## Testing & Quality Gates
```bash
# Linting and formatting
ruff check src tests
ruff format src tests

# Static typing
mypy src

# pre-commit hooks (install & run all checks locally)
pip install pre-commit && pre-commit install
pre-commit run --all-files

# Unit, integration, and performance tests
pytest tests/unit
pytest tests/integration
pytest tests/performance
```


