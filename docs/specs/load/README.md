# Load Specs Overview

| Step                 | Producer                          | Consumer                    | Contract                                                                                   |
| -------------------- | --------------------------------- | --------------------------- | ------------------------------------------------------------------------------------------ |
| S3 Event → Lambda    | EventBridge (S3 `Object Created`) | Load Event Publisher Lambda | `docs/specs/load/load-pipeline-spec.md`                                                    |
| Lambda → SQS         | Load Event Publisher Lambda       | `{env}-{domain}-load-queue` | `docs/specs/load/load-pipeline-spec.md` <br> `docs/specs/load/load-component-contracts.md` |
| Loader → 내부 시스템 | On-Premise/External Loader        | Downstream DB/서비스        | `docs/specs/load/load-component-contracts.md` (LoaderConfig, message schema)               |
