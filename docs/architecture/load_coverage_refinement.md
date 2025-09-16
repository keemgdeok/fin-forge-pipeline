# Load Pipeline Coverage Refinement

## Context

Load 도메인 유닛 테스트만 실행할 때도 `src/glue` 모듈이 import 되는 바람에 0% 커버리지 항목으로 리포트에 노출되는 문제가 있었다. 원인은 `glue/__init__.py`에서 하위 패키지를 즉시 import 했기 때문.

## Decision (Lazy package init)

- `src/glue/__init__.py`에서 하위 모듈을 자동 import 하지 않도록 수정.
- 패키지 사용처는 필요한 서브패키지를 명시적으로 import (e.g. `import glue.jobs`).
- `pyproject.toml`의 coverage 설정에서 `src/glue/lib/*` 및 `src/glue/__init__.py`를 omit 목록에 추가해 Load 전용 테스트 실행 시 False Negative를 방지.

## Benefits

- 불필요한 import 비용 및 커버리지 노이즈 제거.
- 테스트 하위셋 실행 시 의도하지 않은 모듈 커버리지 실패를 방지.

## Follow-up

- Coverage 범위를 도메인별로 선택적으로 지정할 수 있도록 `pytest` 실행 가이드에 --cov 예시 추가.
- CI 단계에서는 전체 테스트를 돌려 전체 커버리지 요건을 만족하도록 유지.
