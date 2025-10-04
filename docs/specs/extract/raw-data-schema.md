# Raw Data Schema — S3 저장 스키마 명세

| 항목 | 내용 |
|------|------|
| 역할 | Extract 단계에서 생성된 RAW 데이터를 저장하는 파티션/스키마 규칙 정의 |
| 코드 기준 | `src/lambda/layers/common/python/shared/ingestion/service.py` |
| 소비자 | Transform Glue Job, Preflight Lambda |

## 버킷 메타데이터

| 항목 | 값 | 설명 |
|------|-----|------|
| 버킷명 패턴 | `data-pipeline-raw-{environment}-{account-suffix}` | 환경/계정별 분리 |
| 리전 | `us-east-1` | Transform과 동일 리전 |
| 암호화 | SSE-S3 (AES256) | 저장 데이터 암호화 |
| 버전 관리 | 활성화 | 실수 삭제 대응 |
| 수명 주기 | 90일 → IA, 365일 → Glacier | 비용 최적화 |

## 파티션 구조

| 계층 | 디렉터리 | 예시 | 설명 |
|------|---------|------|------|
| 1 | `{domain}` | `market` | 비즈니스 도메인 |
| 2 | `{table_name}` | `prices` | 데이터셋 |
| 3 | `interval={interval}` | `interval=1d` | 수집 주기 |
| 4 | `data_source={source}` | `data_source=yahoo_finance` | 공급자 구분 |
| 5 | `year={YYYY}` | `year=2025` | 연도 |
| 6 | `month={MM}` | `month=09` | 월 |
| 7 | `day={DD}` | `day=07` | 일 |
| 파일 | `{symbol}.{ext}[.gz]` | `AAPL.json.gz` | 심볼별 파일 (중복 시 멱등 처리) |

## 파티션 키 효과

| 레벨 | 키 | 카디널리티 | Athena 프루닝 | 비고 |
|:---:|---|:----------:|:-----------:|------|
| 1 | domain | 낮음 | ⭐⭐⭐ | 도메인 필터 |
| 2 | table_name | 낮음 | ⭐⭐⭐ | 데이터셋 필터 |
| 3 | interval | 낮음 | ⭐⭐⭐⭐ | 주기 필터 |
| 4 | data_source | 낮음 | ⭐⭐⭐ | 공급자 구분 |
| 5 | year | 높음 | ⭐⭐⭐⭐ | 날짜 필터 |
| 6 | month | 중간 | ⭐⭐⭐⭐⭐ | 날짜 필터 |
| 7 | day | 높음 | ⭐⭐⭐⭐⭐ | 일별 분석 |

## Price Record 스키마

| 필드명 | 타입 | 널 허용 | 제약 조건 | 예시 |
|--------|------|:-------:|----------|------|
| `symbol` | string | ❌ | 1–20자, 영숫자/`.`/`-` | `AAPL` |
| `timestamp` | timestamp | ❌ | ISO 8601 UTC | `2025-09-09T09:30:00Z` |
| `open` | decimal(10,2) | ✅ | ≥ 0 | `150.25` |
| `high` | decimal(10,2) | ✅ | ≥ 0 | `152.10` |
| `low` | decimal(10,2) | ✅ | ≥ 0 | `149.80` |
| `close` | decimal(10,2) | ✅ | ≥ 0 | `151.45` |
| `volume` | bigint | ✅ | ≥ 0 | `1234567` |
| `adj_close` | decimal(10,2) | ✅ | ≥ 0 | `151.45` |

### 형식별 타입 매핑

| 형식 | `symbol` | `timestamp` | 가격 필드 | `volume` |
|------|----------|-------------|-----------|----------|
| JSON | string | string (ISO) | number | integer |
| CSV | text | text | text | text |
| Parquet | UTF8 | TIMESTAMP_MILLIS | DOUBLE | INT64 |
| Athena | string | timestamp | decimal(10,2) | bigint |

## 파일 형식 특성

| 형식 | 확장자 | 압축률 | 호환성 | 쿼리 성능 | 권장 용도 |
|------|--------|:------:|:------:|:---------:|----------|
| JSON Lines | `.json` | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | 기본값 |
| CSV | `.csv` | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐ | 수동 분석 |
| Parquet (미지원) | - | - | - | - | Transform 단계에서 변환 |

## 파일 크기 가이드

| 형식 | 목표 크기 | 최대 크기 | 심볼당 30일 예상 |
|------|:---------:|:---------:|:---------------:|
| JSON | 1–10MB | 50MB | ~9MB |
| CSV | 1–5MB | 25MB | ~6MB |
| Parquet (추후) | 10–100MB | 200MB | ~3MB |

## 데이터 품질 체크리스트

| 항목 | 조건 | 액션 | 로그 |
|------|------|------|------|
| 필수 필드 | `symbol`, `timestamp` 존재 | 레코드 스킵 | ERROR |
| 심볼 형식 | 정규식 `^[A-Z0-9._-]{1,20}$` | 레코드 스킵 | WARNING |
| 타임스탬프 | ISO 8601 UTC | 레코드 스킵 | ERROR |
| 가격/볼륨 | 음수 방지 | 경고 후 계속 | WARNING |

## S3 객체 메타데이터

| 태그 키 | 값 예시 | 용도 |
|---------|---------|------|
| `ingestion_timestamp` | `2025-09-09T14:30:45Z` | 추적 |
| `data_source` | `yahoo_finance` | 필터링 |
| `symbol_count` | `3` | 청크 통계 |
| `record_count` | `90` | 청크 통계 |
| `file_format` | `json` | 파일 형식 구분 |

## 압축 정책

| 방식 | 압축률 | CPU 비용 | 적용 대상 |
|------|:------:|:--------:|-----------|
| GZIP | 75–80% | ⭐⭐⭐ | JSON, CSV (stg/prod) |
| ZSTD | 80–85% | ⭐⭐ | 대용량 JSON (옵션) |
| 없음 | 0% | ⭐⭐⭐⭐⭐ | Dev 디버깅 |

### 환경별 압축 기본값

| 환경 | JSON | CSV | 비고 |
|------|------|-----|------|
| dev | ❌ | ❌ | 디버깅 우선 |
| staging | ✅ (GZIP) | ✅ (GZIP) | 운영 리허설 |
| prod | ✅ (GZIP) | ✅ (GZIP) | 비용 최적화 |

---

| 관련 문서 | 경로 |
|----------|------|
| Orchestrator 계약 | `docs/specs/extract/orchestrator-contract.md` |
| Worker 계약 | `docs/specs/extract/worker-contract.md` |
| SQS 통합 | `docs/specs/extract/sqs-integration-spec.md` |
