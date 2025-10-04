# Load Optimization Guidelines (Pull Model)

## SQS & Loader 최적화 설정

### **SQS 설정**
- **MaxNumberOfMessages**: 5–10 (배치 폴링)
- **WaitTimeSeconds**: 20 (롱 폴링로 빈폴 비용 최소화)
- **VisibilityTimeout**: per‑message timeout × 6 (권장: 1800초)
- **maxReceiveCount**: 3 (DLQ 이동 전)

### **Loader 동시성/리소스**
- **Worker 수**: 5–20 (큐 백로그와 CH 리소스에 맞춰 조절)
- **Per‑message Timeout**: 30–300초 (데이터 크기/도메인별 상이)
- **Prefetch/버퍼링**: 폴링→처리 파이프라인 비동기 구성 권장
- **ACK 정책**: 성공 시 즉시 Delete, 실패 시 가시성 연장 또는 미삭제

## ClickHouse s3() 기반 INSERT 최적화

### **입력 파일 전략**
- **경로 정합성**: 현재 Transform 출력은 `interval=…/data_source=…/year=…/month=…/day=…/layer=<name>/` 구조이므로 로더가 예상 경로(`ds=`)에 맞게 변환하거나 Lambda를 수정해야 합니다.
- **파티션 선택**: 일자별 처리 시 `year=YYYY/month=MM/day=DD/` 기반 와일드카드를 활용
- **대용량**: 파티션별 파일 크기 128–512MB 권장(Gluespec와 일치)
- **클러스터 병렬화**: (선택) `Distributed` 테이블로 샤드 병렬 적재

### **연결/쿼리 관리**
- **Backoff**: 2s→4s→8s (일시 오류 재시도 최대 3회)
- **Timeout**: `QUERY_TIMEOUT` 30초 기준, 데이터 크기에 따라 상향
- **메모리**: Parquet 해제/매핑 비용 고려, 프로세스 메모리 모니터링

## 성능 모니터링 지표

### **처리량/지연 목표**
- **시간당 처리**: 10K–50K rows 등 업무 요구 기준
- **엔드투엔드 지연**: S3 Object Created → CH INSERT < 5분
- **오류율**: < 1% (DLQ 포함)

참조: 03-sequence.md (전체 플로우), 06-monitoring-metrics.md (상세 메트릭)
