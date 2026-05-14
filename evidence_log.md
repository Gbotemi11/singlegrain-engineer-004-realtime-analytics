# Evidence Log

| Claim | Number label | Proof tier | Evidence/source |
|---|---:|---:|---|
| Challenge says 50M events/day, 15–30 min latency, ~3% peak loss, $50K/month ceiling, AWS-only, 500+ customers | Brief-observed | 3 | Engineer 004 challenge brief |
| Average events/sec = 579 | Estimated | 3 | `load_and_cost_model.py` |
| 10x spike events/sec = 5,787 | Estimated | 3 | `load_and_cost_model.py` |
| Average event size = 1.5KB | Assumed | 2 | Stated assumption; should be replaced by real sampled event payloads |
| 10x spike ingest = 8.68MB/sec | Estimated | 3 | `load_and_cost_model.py` |
| Kinesis provisioned shard write capacity = 1,000 records/sec or 1MB/sec | Benchmarked | 3 | AWS Kinesis Data Streams docs |
| Kinesis on-demand default begins at 4MB/sec write and 4,000 records/sec write | Benchmarked | 3 | AWS Kinesis Data Streams docs |
| ClickHouse recommends inserts of at least 1,000 rows, ideally 10,000–100,000 rows | Benchmarked | 3 | ClickHouse docs |
| Iceberg/Athena can support delete operations for data lake records | Benchmarked | 3 | AWS Athena/Iceberg docs |
| Design remains below $50K/month | Estimated | 2 | Guardrail model only; final AWS Pricing Calculator required |
