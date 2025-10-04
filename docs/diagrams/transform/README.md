# Transform Diagrams Overview

<table style="width:100%; table-layout:fixed;">
  <thead>
    <tr>
      <th style="width:12%;">Diagram</th>
      <th style="width:68%;">미리보기</th>
      <th style="width:20%;">설명</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>Components</td>
      <td><img src="01-components-1.svg" alt="Components" style="width:100%;" /></td>
      <td>상태 머신과 관련 리소스 구성요소</td>
    </tr>
    <tr>
      <td>Flow</td>
      <td><img src="02-flow-1.svg" alt="Flow" style="width:100%;" /></td>
      <td>맵 상태를 통한 Preflight→Glue→Crawler 흐름</td>
    </tr>
    <tr>
      <td>Sequence</td>
      <td><img src="03-sequence-1.svg" alt="Sequence" style="width:100%;" /></td>
      <td>단일 manifest 항목 처리 시퀀스</td>
    </tr>
    <tr>
      <td>Data Quality Gate</td>
      <td><img src="04-data-quality-gate-1.svg" alt="DQ Gate" style="width:100%;" /></td>
      <td>Glue ETL 내 품질 검사/격리 경로</td>
    </tr>
    <tr>
      <td>Backfill Map</td>
      <td><img src="05-backfill-map-1.svg" alt="Backfill" style="width:100%;" /></td>
      <td>manifest 기반 Map 상태 구조</td>
    </tr>
    <tr>
      <td>Glue Internals</td>
      <td><img src="06-glue-job-internals-1.svg" alt="Glue Internals" style="width:100%;" /></td>
      <td>Glue Job 단계별 작업 요약</td>
    </tr>
    <tr>
      <td>IO &amp; Schema (Curated)</td>
      <td><img src="07-io-and-schema-1.svg" alt="IO Curated" style="width:100%;" /></td>
      <td>RAW→Curated 경로와 파티션 구조</td>
    </tr>
    <tr>
      <td>IO &amp; Schema (Class)</td>
      <td><img src="07-io-and-schema-2.svg" alt="IO Class" style="width:100%;" /></td>
      <td>입력/출력 레코드 필드 매핑</td>
    </tr>
    <tr>
      <td>Catalog Strategy</td>
      <td><img src="08-catalog-strategy-1.svg" alt="Catalog" style="width:100%;" /></td>
      <td>크롤러/테이블 전략 결정 트리</td>
    </tr>
  </tbody>
</table>
