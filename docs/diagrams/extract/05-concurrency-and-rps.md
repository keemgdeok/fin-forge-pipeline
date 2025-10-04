```mermaid
flowchart TD
  A["Inputs<br/>k = orchestrator_chunk_size<br/>c = worker_reserved_concurrency<br/>t_msg = avg seconds/message"]
  B["Messages per run<br/>≈ N / k"]
  C["External RPS<br/>≈ c / t_msg"]
  D["Tickers per second<br/>≈ (c / t_msg) × k"]
  E["Total time T<br/>≈ N × t_msg / (c × k)"]

  A --> B
  A --> C
  A --> D
  A --> E
  B --> E

  subgraph Tuning
    F["Lower cost:<br/>Increase k (until t_msg growth outweighs gains)"]
    G["Protect API:<br/>Decrease c or set YF_THREADS=false"]
    H["Speed up:<br/>Increase c (keep RPS ≤ ~3)"]
    I["Heavy intervals (e.g., 1m):<br/>Decrease k, increase memory"]
  end

  C --> G
  D --> H
  B --> F
  A --> I

  N1["환경 설정에서 제어:<br/>`orchestrator_chunk_size`, `worker_reserved_concurrency`(0이면 제한 없음)"]
  N2["운영 권장: 외부 API RPS 1–3 req/s 내 관리"]
  A -.-> N1
  C -.-> N2
```
