# Transform Specs Overview

| Step                      | Producer              | Consumer                                    | Contract                                         |
| ------------------------- | --------------------- | ------------------------------------------- | ------------------------------------------------ |
| Step Functions 입력       | Runner / Orchestrator | Transform State Machine                     | `docs/specs/transform/state-machine-contract.md` |
| State Machine → Glue Jobs | Step Functions        | Glue Compaction / ETL / Indicators Jobs     | `docs/specs/transform/glue-job-spec.md`          |
| Glue Jobs 출력            | Glue Jobs             | Curated / Indicators S3, Schema Fingerprint | `docs/specs/transform/glue-job-spec.md`          |
