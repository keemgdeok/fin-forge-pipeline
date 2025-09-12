# 🚀 CI/CD 가이드

**193줄로 최적화된 GitHub Actions - 소규모 팀용**

## 📋 워크플로우 구성

| 파일 | 용도 | 시간 | 라인 |
|------|------|------|------|
| `ci.yml` | 품질검사 + 테스트 | 2-4분 | 77줄 |
| `deploy.yml` | 자동 배포 | 3-8분 | 54줄 |
| `pr-check.yml` | PR 빠른 검증 | 1-2분 | 62줄 |

## 🏗️ CI 워크플로우

**트리거:** `main`, `develop` 푸시 또는 PR

**병렬 매트릭스 실행:**
- `format` - Ruff Formatter + Ruff Lint 스타일 체크
- `test` - pytest 단위 테스트
- `infra` - CDK 인프라 검증  
- `security` - 보안 스캔 (조건부)

**특징:** 4개 작업 동시 실행으로 속도 3배 향상

## 🚀 배포 워크플로우

**스마트 환경 선택:**
- `develop` → `dev` 자동 배포
- `main` → `prod` 자동 배포
- 수동 실행 → 환경 선택 가능

**배포 흐름:** Bootstrap → CDK Diff → Deploy → 완료

**환경 설정:**
- `dev`: 자동 배포
- `prod`: 수동 승인 필요

## 🔍 PR 체크 워크플로우

**변경 기반 검증:**
- Python 파일 → 포맷팅 + 린팅
- 인프라 파일 → CDK 문법 검사
- 변경 없음 → 즉시 통과

**최적화:** 변경된 파일만 체크, 필요한 도구만 설치

## ⚙️ 설정

**GitHub Secrets:** `Settings > Secrets and variables > Actions`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

**GitHub Environments:** `Settings > Environments`
- `dev`: 자동 배포 (보호 규칙 없음)
- `prod`: 수동 승인 필요 (리뷰어 1명)

## 🎯 사용법

**개발 워크플로우:**
1. 기능 개발: `git checkout -b feature/new-feature`
2. PR 생성: 자동 검증 실행
3. 병합: `develop` → `dev` 자동 배포, `main` → `prod` 자동 배포

**수동 배포:** GitHub Actions > Deploy 워크플로우 > Run workflow

**보안 스캔:** 커밋 메시지에 `[security]` 포함 또는 `main` 브랜치 푸시

## 🔧 문제 해결

**포맷팅 에러:**
```bash
ruff format src/ infrastructure/ tests/
git add -A && git commit -m "fix: format code"
```

**CDK 문법 에러:**
```bash
cdk synth --context environment=dev
```

**배포 실패:** CloudFormation 콘솔 확인 또는 `cdk diff` 로컬 디버깅

## 📊 성능 개선

| 대상 | 이전 | 현재 | 개선 |
|------|------|------|------|
| 워크플로우 | 5개 | 3개 | 40% 감소 |
| 코드 라인 | 1015줄 | 193줄 | 81% 감소 |
| 실행 시간 | 8-15분 | 2-8분 | 60% 단축 |
| Actions 비용 | - | - | 70% 절약 |

**병렬 실행 효과:** 순차 → 병렬로 전환하여 60-70% 단축

## 🎯 소규모 팀 최적화

**단순함:**
- 3개 파일, 각 60-80줄
- 즉시 수정 가능한 명확한 에러 메시지

**빠른 속도:**
- 1-2분 PR 체크
- 병렬 매트릭스 실행
- 조건부 실행

**실용성:**
- 자동 환경 선택 (`develop`→`dev`, `main`→`prod`)
- Smart CDK Bootstrap 체크
- 단계별 수정 가이드

## 🚨 주의사항

**AWS 비용:** `aws cloudformation list-stacks` 로 리소스 정기 확인

**보안:** 액세스 키 주기적 갱신, 최소 권한 원칙 준수



## 📈 향후 확장

**팀 규모 증가시 (5-10명):** Slack 알림, 메트릭스, staging 환경

**프로젝트 성숙시:** 통합테스트, 성능테스트, 승인 프로세스
