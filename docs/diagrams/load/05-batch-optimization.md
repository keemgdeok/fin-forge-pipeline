# Load Optimization Guidelines

## SQS & Lambda 최적화 설정

### **SQS 배치 설정**
- **배치 크기**: 5-10개 메시지 (최적)
- **Visibility Timeout**: Lambda timeout × 6 
- **maxReceiveCount**: 3 (DLQ 이동 전)

### **Lambda 동시성 설정**  
- **예약 동시성**: 5-10 (안정적 처리)
- **메모리**: 512MB-1GB (Parquet 처리용)
- **타임아웃**: 5분 (DW 연결 고려)

## Data Warehouse INSERT 최적화

### **배치 크기 전략**
- **< 1MB 데이터**: Single INSERT (지연시간 최소화)
- **> 1MB 데이터**: Batch INSERT (1K-5K rows)
- **압축 활용**: Parquet → 메모리 효율성 극대화

### **연결 관리**
- **Connection Pooling**: DW 연결 재사용
- **재시도 정책**: Exponential Backoff (2s, 4s, 8s)
- **타임아웃**: 30초 (DW 응답 대기)

## 성능 모니터링 지표

### **처리량 목표**
- **시간당 처리**: 10K-50K rows
- **지연시간**: S3 Object Created → DW INSERT < 5분
- **오류율**: < 1% (DLQ 포함)

참조: 03-sequence.md (전체 플로우), 06-monitoring.md (상세 메트릭)