# Catalog Strategy Decision Tree (Mermaid)

```mermaid
flowchart TD
  A["목표: Curated를 카탈로그에 노출"] --> B{스키마 안정성 높은가?}
  B -->|Yes| C["IaC로 Glue Table 정의<br/>- 스키마/파티션 키 고정<br/>- StorageDescriptor 경로 지정"]
  B -->|No| D{스키마 변화 빈번/예측 어려움?}

  D -->|Yes| E["Glue Crawler 사용<br/>- 신규 경로만 크롤<br/>- 스키마 최소 수정 허용<br/>- 비용/지연 모니터링"]
  D -->|No| F{파티션 생성 빈번/대량인가?}

  F -->|Yes| G["Athena 파티션 프로젝션<br/>- `projection.enabled=true`<br/>- `projection.ds.type=date`<br/>- `storage.location.template` 설정"]
  F -->|No| H["IaC Table + 수동 파티션 추가<br/>(또는 ETL 후 `MSCK REPAIR TABLE`)" ]

  %% 공통 권장 사항
  N1["권한 관리: Glue Catalog + IAM<br/>컬럼 제한은 Athena View로 구현"]:::note
  N2["S3/KMS 암호화 일관성<br/>Athena 결과 버킷 암호화"]:::note
  N3["크롤러 사용 시<br/>스키마 변경 감지 시에만 실행<br/>스코프는 '신규 경로'로 제한"]:::note

  C -.-> N1
  E -.-> N3
  G -.-> N2

  classDef note fill:#fff3cd,stroke:#d39e00,color:#5c4800;
```

비고

- IaC 테이블 정의: 스키마가 안정적일 때 최선. 파티션 키(`ds`)와 S3 경로를 코드로 고정하여 예측 가능한 쿼리/권한 모델을 유지합니다.
- 파티션 프로젝션: 파티션 생성이 잦거나 대량일 때 비용/지연을 크게 줄입니다. 날짜 범위 쿼리에 특히 유리합니다.
- 크롤러: 스키마 변화가 잦을 때만 제한적으로 사용하고, 상태 머신에서 스키마 지문 비교로 변경을 감지한 경우에만 실행합니다. 대상은 신규 경로로 제한하여 비용/지연을 통제합니다. 구현 시 `RecrawlPolicy=CRAWL_NEW_FOLDERS_ONLY` 설정을 권장합니다.
