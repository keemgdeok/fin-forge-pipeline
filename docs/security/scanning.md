# Security Scanning Playbook

이 문서는 Bandit(코드 보안 정적 분석)과 Safety(의존성 취약점 스캔)를 운영하기 위한 표준 절차입니다. 로컬 개발자 워크플로, CI/CD 통합, 예외 처리 및 장애 대응까지 포함해 "어디서 무엇을 어떻게" 수행해야 하는지 정리했습니다.

## 도구 구성 개요
- **Bandit**: `bandit.yaml` 정책을 바탕으로 `src/`, `infrastructure/`, `scripts/`를 재귀적으로 검사합니다. 기본 심각도/신뢰도 기준은 `medium`입니다.
- **Safety**: 현재 활성화된 가상환경을 기반으로 설치된 패키지 목록에서 CVE 및 OSV 취약점을 탐지합니다.
- **버전 고정**: `requirements.txt`에 `bandit==1.7.10`, `safety==2.3.5`를 명시해 CI/로컬 간 결과 차이를 최소화합니다. 필요 시 `pip-compile` 기반 잠금 전략으로 확장할 수 있습니다.

## 로컬 사전 점검(pre-commit)
1. 최초 1회 설정: `pip install pre-commit && pre-commit install`.
2. 수동 전체 검사: `pre-commit run --all-files` (Bandit 포함, Safety는 실행 시간 관계로 제외되어 있습니다). Bandit 훅은 `--severity-level medium --confidence-level medium`을 사용해 중간 이상 이슈만 실패를 유발합니다.
3. 특정 훅만 재실행: `pre-commit run bandit-security-audit --all-files`.
4. 긴급 상황에서 임시 스킵이 필요하면 `SKIP=bandit-security-audit pre-commit run`을 사용할 수 있지만, 보안 담당자 승인 없이 스킵된 커밋은 허용되지 않습니다.

> Safety 훅은 실행 시간이 길고 네트워크 종속성이 커서 pre-commit 단계에서는 기본적으로 비활성화되어 있습니다. 팀에서 필요성을 합의하면 `repo: pyupio/safety` 훅을 추가하거나 로컬 스크립트 훅으로 전환하세요.

## PR 검증(필수)
- 워크플로: `.github/workflows/pr-check.yml`
- 트리거: Python 또는 인프라 파일이 변할 때 자동 실행됩니다.
- 동작
  - 의존성 설치 후 `Security audit (bandit)` 단계에서 JSON 보고서를 생성하고, 상위 10개 이슈 요약을 Summary에 남깁니다. 동일한 `--severity-level medium --confidence-level medium` 기준을 사용해 Low 레벨 알림은 경고로만 남습니다.
  - `Dependency vulnerability scan (safety)`는 현재 환경을 검사해 전체 리포트를 Summary에 첨부합니다(`--output text`로 ANSI 색상 제거).
  - 두 단계 모두 **취약점이 발견되면 실패(exit-on-error)** 하며, Summary에서 상세 내역을 바로 확인할 수 있습니다.
- 재실행: 취약점 조치 후 `git commit --amend` 또는 `git commit` → force-push로 워크플로를 재시작하세요.

## 배포(full pipeline) 시 보안 스캔
- 워크플로: `.github/workflows/deploy.yml`
- full pipeline 모드일 때만 실행되며, PR 단계에서 이미 차단된 이슈를 재확인하는 **요약용(non-blocking)** 스캔입니다.
- 결과는 GitHub Step Summary에만 기록되고, 실패하더라도 배포 작업은 계속 수행됩니다.
- 운영팀은 Summary를 통해 잔존 리스크를 공유하고, 필요 시 핫픽스 PR을 생성합니다.

## 의존성 및 캐시 관리 전략
- **pip 캐시**: PR/배포 워크플로는 `actions/setup-python`의 `cache: 'pip'` 설정을 사용해 `~/.cache/pip`를 자동 캐싱합니다. `requirements.txt` 해시가 키가 되므로 의존성 변경 시 자동으로 캐시가 무효화됩니다.
- **pre-commit 캐시**: 개발자는 `~/.cache/pre-commit`를 정기적으로 정리(`pre-commit gc`)해 오래된 Bandit/Safety 버전을 제거하세요.
- **pip-tools 연동**: 장기적으로 `requirements.in` → `pip-compile`로 고정한다면, Safety 스캔은 `requirements.txt` 대신 `requirements.lock`을 대상으로 실행하도록 워크플로를 업데이트해야 합니다.
- **내부 아티팩트**: 배포 파이프라인의 Lambda Layer(`src/lambda/layers/market_data_deps/python`)는 Bandit 대상에서 제외되어 있으며, 별도로 취약점 검토가 필요하면 Safety `--file` 옵션에 해당 requirements를 추가하세요.

## 예외 처리(오탐) 프로세스
1. **이슈 제출**: Jira/이슈 트래커에 "Security scanning false positive" 유형으로 등록하고, 재현 경로와 근거를 첨부합니다.
2. **Bandit**
   - 일시적으로 허용하려면 `bandit.yaml`의 `skips`에 `BXXX` 규칙 ID를 추가하거나, 특정 파일에서 `# nosec` 주석을 사용합니다.
   - `# nosec` 주석은 최소 범위(해당 라인)로 제한하고, 주석 옆에 링크(이슈 번호)를 남깁니다.
   - 변경 PR은 반드시 보안 담당자 리뷰를 받아야 하며, 3개월마다 예외 항목을 재검토합니다.
3. **Safety**
   - 단일 항목 무시는 `safety check --ignore <vuln_id>` 또는 `pyproject.toml`에 `[tool.safety] ignore = ['<id>']` 형식으로 관리합니다.
   - 만료일을 명시하고, 대체 패치 계획(업그레이드 버전, 패치 릴리스 등)을 함께 기록합니다.
4. **문서 업데이트**: 모든 예외는 본 문서 또는 팀 위키에 최신 상태로 반영해 추적 가능성을 유지합니다.

## 장애 대응 및 개발자 가이드
- **Bandit 실패 시**
  - 메시지에 나온 파일/라인을 확인하고 안전한 패턴으로 수정합니다.
  - 외부 라이브러리(예: `subprocess`, `pickle`) 사용 시 입력 검증 또는 안전한 대안을 채택하세요.
  - 재현: `bandit -c bandit.yaml -r src infrastructure scripts`.
- **Safety 실패 시**
  - `safety report --output text`로 추가 정보를 확인합니다.
  - 패키지 업그레이드 후 `pip freeze > requirements.lock` 등으로 변경 내역을 기록하세요.
  - 인적 검토가 필요한 경우 보안팀과 짝지어 영향도를 평가합니다.
- **도구 재설치**
  - 가상환경 손상 시 `rm -rf .venv && python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`.
  - 네트워크 문제로 Safety API 호출이 실패하면 VPN/프록시 설정을 확인하고, 일회성 재시도 후에도 실패한다면 이슈를 남기세요.
- **CI에서 강제 재실행**
  - GitHub UI의 `Re-run failed jobs`를 사용하거나, 빈 커밋 `git commit --allow-empty -m "ci: re-run security scan"` 후 푸시합니다.

## 온보딩 체크리스트 업데이트
- [ ] Python 3.12 또는 대응 버전 설치
- [ ] `python -m venv .venv && source .venv/bin/activate`
- [ ] `pip install -r requirements.txt`
- [ ] `pre-commit install`
- [ ] `pre-commit run --all-files`로 Bandit/Ruff/Mypy 초기 정리
- [ ] CI 실패 시 이 문서의 "장애 대응" 섹션을 참고하도록 안내
- [ ] 팀 위키/README에 본 문서를 링크해 항상 최신 절차를 공유

문의 또는 개선 제안은 #security-ops 슬랙 채널 혹은 PR 코멘트로 남겨주세요.
