# Engineer 004 Operating Test Plan

## 1. Ingest durability test

**Goal:** prove no accepted event is lost after collector acknowledgement.

Steps:
1. Generate synthetic events for 20 tenants with uniform traffic.
2. Generate synthetic events for 1 hot tenant with 40% of traffic.
3. Force Kinesis partial write failures.
4. Confirm failed records enter SQS spillover.
5. Drain SQS and compare accepted event IDs against S3/Iceberg and ClickHouse.

Pass condition:
- 100% of collector-accepted event IDs appear in the replayable source of truth.
- Duplicates are classified and deduped by `event_id`.

## 2. Latency test

**Goal:** prove <5s freshness for dashboard aggregates under normal load.

Measurements:
- `collector_received_ts`
- `kinesis_arrival_ts`
- `flink_processed_ts`
- `clickhouse_visible_ts`
- `dashboard_render_ts`

Pass condition:
- p95 event-to-dashboard freshness <5s for priority dashboard cards during non-degraded state.

## 3. Spike test

**Goal:** validate 10x traffic without silent loss.

Scenarios:
- Uniform 10x traffic.
- Hot-tenant 10x traffic.
- Hot-event-type traffic.
- Collector restart during spike.
- ClickHouse insert slowdown during spike.

Pass condition:
- Collector does not acknowledge events that are not durably written.
- SQS spillover drains after recovery.
- Dashboard shows degraded freshness instead of wrong freshness.

## 4. Data parity test

**Goal:** prove new pipeline agrees with old pipeline before tenant cutover.

Checks:
- total event count by tenant/day;
- count by event type;
- count by 5-minute bucket;
- sampled payload-level comparison;
- dashboard aggregate comparison.

Pass condition:
- discrepancies are classified: old-system loss, new-system transform bug, late events, duplicate events, or expected semantic difference.
- no tenant is cut over while unexplained discrepancy remains.

## 5. GDPR/CCPA deletion test

**Goal:** prove a user deletion request removes query-visible personal data.

Steps:
1. Create synthetic user and anonymous visitor.
2. Emit page views, clicks, identify/login event, and custom event.
3. Confirm visibility in Redis, ClickHouse, and Iceberg/Athena.
4. Submit deletion request.
5. Verify deletion ledger transitions to complete.
6. Query all stores again.

Pass condition:
- Redis keys absent.
- ClickHouse hot rows deleted or masked.
- Iceberg/Athena query excludes deleted rows.
- Export pipeline does not re-export deleted identity.
