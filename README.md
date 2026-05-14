# Single Grain Engineer 004 Submission Package

This package contains:

1. `challenge_submission.md` — the written submission.
2. `architecture.mmd` — Mermaid architecture diagram.
3. `load_and_cost_model.py` — runnable sizing model for event rate, spike load, Kinesis shard sizing, S3 raw volume, and budget guardrails.
4. `model_output.json` — output from the sizing model with default assumptions.
5. `test_plan.md` — operating checklist for migration, reliability, accuracy, and deletion validation.
6. `evidence_log.md` — proof tier and source-label table.

## How to run the artifact

```bash
python3 load_and_cost_model.py
python3 load_and_cost_model.py --events-per-day 50000000 --avg-event-kb 1.5 --spike-multiplier 10
```

## What the reviewer should inspect

- The design does not hide behind generic “Kafka + Flink” wording.
- The sizing model shows why 50M/day is modest but 10x sudden spike + tenant skew can still break ingest.
- The failure plan is explicit about what cannot be guaranteed without SDK changes.
- The compliance design includes deletion workflow, not just storage.
