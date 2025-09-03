# 테스트 규칙과 실행 가이드

이 문서는 로컬 작업부터 PR/Push 시 CI까지 테스트·품질 점검 규칙을 한눈에 정리합니다.

## TL;DR 체크리스트
- 변경 사항 커밋 전:
  - `pre-commit run --all-files` (처음 1회 `pre-commit install` 필요)
  - 인프라 변경 시: `npx cdk synth --context environment=dev`로 합성 확인
  - 테스트가 있다면: `pytest -q`
- 커밋 후: 일반 커밋은 훅이 자동 실행됩니다. Black이 파일을 수정하면 재-`git add` 후 커밋하세요.

## CI 규칙(자동 검사)
- 워크플로: `.github/workflows/pr-check.yml`
- 트리거: `pull_request`(대상 브랜치: `main`, `develop`)
- 공통: Node 20, Python 3.12, 변경 파일 기반 단계 실행, 이전 작업 자동 취소(concurrency)
- 문서 전용 변경은 스킵: `**/*.md`, `docs/**`

변경 유형별 동작
- Python 파일 변경 시
  - 설치: `pip install -r requirements.txt`
  - 포맷/린트: `black --line-length 120 --check`, `flake8 --max-line-length 120 --extend-ignore E203,W503`
  - 타입체크: `mypy --ignore-missing-imports`
  - 테스트: `tests/` 존재할 때 `pytest` 빠른 실행
- 인프라(`infrastructure/`) 변경 시
  - Node 20 설정 + CDK CLI 설치
  - 합성: `cdk synth --context environment=dev` (로그 숨김 없음)

배포 워크플로(`.github/workflows/deploy.yml`)
- 트리거: `push`(main/develop) 또는 수동 `workflow_dispatch`
- OIDC AssumeRole 사용(권장), Node 20, `npx cdk`로 synth/diff/deploy
- `docs/**`, `**/*.md`는 무시, 동시 실행 취소, prod는 Environments에서 승인 게이트 설정 권장

## 로컬 실행 가이드
- 가상환경/의존성
  - `python -m venv .venv && source .venv/bin/activate`
  - `pip install -r requirements.txt`
- 품질 점검(체크 전용)
  - `black --line-length 120 --check --diff .`
  - `flake8 --max-line-length 120 --extend-ignore E203,W503 .`
  - `mypy --ignore-missing-imports .`
- 인프라 합성
  - `export CDK_DEFAULT_ACCOUNT=<12자리계정ID>`
  - `export CDK_DEFAULT_REGION=ap-northeast-2` (또는 환경에 맞게)
  - `npx cdk synth --context environment=dev`

## pre-commit 훅(권장)
- 설정 파일: `.pre-commit-config.yaml`
- 포함 훅
  - Black: `--line-length 120` (자동 포맷)
  - Flake8: `--max-line-length 120 --extend-ignore E203,W503`
  - mypy: `--ignore-missing-imports`
- 사용법
  - 설치: `pip install pre-commit`
  - 활성화: `pre-commit install`
  - 전체 실행: `pre-commit run --all-files`
  - 커밋 시 자동 실행: `git commit` 시 변경 파일만 검사/포맷

## 실패 대응 팁
- Black가 파일을 수정해 실패할 때: 수정된 파일을 `git add` 후 다시 실행/커밋
- Flake8 E501(라인 길이) 관련: 기준 120자, 필요한 경우 줄바꿈 또는 문자열 분리
- mypy 오류: 함수 시그니처 타입힌트 보완, `Optional` 널 가드, `from_role_arn` 등 명시 타입 사용
- CDK synth 실패: Node 20 사용, CDK CLI 최신, `definition_body` API 사용, 컨텍스트/환경 변수 확인

## 기타 규칙
- 라인 길이: 120자(Black/Flake8 공통)
- 무시 규칙: E203, W503 (Black과 호환)
- .gitignore 권장: `cdk.out/`, `node_modules/`는 커밋 금지, `package-lock.json`은 커밋 유지

문의/개선 제안은 PR 또는 이슈로 남겨주세요.✨
