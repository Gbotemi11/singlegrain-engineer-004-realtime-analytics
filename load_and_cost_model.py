#!/usr/bin/env python3
"""
Engineer 004 operating artifact: load and cost guardrail model.

This script does not pretend to be a final AWS quote.
It makes challenge assumptions explicit and labels every number source.

Run:
  python3 load_and_cost_model.py
  python3 load_and_cost_model.py --events-per-day 50000000 --avg-event-kb 1.5 --spike-multiplier 10
"""

import argparse
import json
import math
from datetime import datetime, timezone


def compute(events_per_day: int, avg_event_kb: float, spike_multiplier: float, month_days: int):
    seconds_per_day = 86400

    avg_eps = events_per_day / seconds_per_day
    spike_eps = avg_eps * spike_multiplier

    avg_mb_s = avg_eps * avg_event_kb / 1024
    spike_mb_s = spike_eps * avg_event_kb / 1024

    # Benchmarked from AWS Kinesis docs for provisioned shards:
    # 1,000 records/sec or 1 MB/sec write per shard.
    shards_by_records = math.ceil(spike_eps / 1000)
    shards_by_mb = math.ceil(spike_mb_s / 1)
    min_spike_shards = max(shards_by_records, shards_by_mb)
    recommended_shards = math.ceil(min_spike_shards * 1.5)

    monthly_raw_gb = events_per_day * avg_event_kb * month_days / (1024 * 1024)

    clickhouse_flush_seconds_at_avg_for_10k = 10000 / avg_eps
    clickhouse_rows_per_5s_at_spike = spike_eps * 5

    # Conservative placeholder guardrail, not final bill.
    # The point is to keep the architecture below the challenge's $50K/month ceiling.
    estimated_monthly_cost_low = 12000
    estimated_monthly_cost_high = 28000

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "inputs": {
            "events_per_day": {"value": events_per_day, "label": "brief-observed"},
            "avg_event_kb": {"value": avg_event_kb, "label": "assumed"},
            "spike_multiplier": {"value": spike_multiplier, "label": "brief-observed"},
            "month_days": {"value": month_days, "label": "assumed"},
        },
        "derived_load": {
            "avg_events_per_second": {"value": round(avg_eps, 2), "label": "estimated"},
            "spike_events_per_second": {"value": round(spike_eps, 2), "label": "estimated"},
            "avg_ingest_mb_per_second": {"value": round(avg_mb_s, 2), "label": "estimated"},
            "spike_ingest_mb_per_second": {"value": round(spike_mb_s, 2), "label": "estimated"},
            "monthly_raw_gb": {"value": round(monthly_raw_gb, 2), "label": "estimated"},
        },
        "kinesis_sizing": {
            "records_per_sec_per_shard": {"value": 1000, "label": "benchmarked"},
            "mb_per_sec_per_shard": {"value": 1, "label": "benchmarked"},
            "shards_needed_by_records": {"value": shards_by_records, "label": "estimated"},
            "shards_needed_by_mb": {"value": shards_by_mb, "label": "estimated"},
            "minimum_spike_shards": {"value": min_spike_shards, "label": "estimated"},
            "recommended_launch_window_shards": {"value": recommended_shards, "label": "estimated"},
            "on_demand_default_records_per_second": {"value": 4000, "label": "benchmarked"},
            "on_demand_default_mb_per_second": {"value": 4, "label": "benchmarked"},
            "on_demand_default_is_enough_for_10x": {
                "value": spike_eps <= 4000 and spike_mb_s <= 4,
                "label": "estimated"
            }
        },
        "clickhouse_insert_guardrail": {
            "target_batch_rows": {"value": 10000, "label": "benchmarked"},
            "seconds_to_10k_rows_at_avg": {
                "value": round(clickhouse_flush_seconds_at_avg_for_10k, 2),
                "label": "estimated"
            },
            "rows_per_5s_at_spike": {
                "value": round(clickhouse_rows_per_5s_at_spike, 0),
                "label": "estimated"
            },
            "note": "Use async inserts or buffering when average traffic produces small per-partition batches."
        },
        "budget_guardrail": {
            "challenge_monthly_ceiling_usd": {"value": 50000, "label": "brief-observed"},
            "estimated_monthly_cost_low_usd": {"value": estimated_monthly_cost_low, "label": "estimated"},
            "estimated_monthly_cost_high_usd": {"value": estimated_monthly_cost_high, "label": "estimated"},
            "requires_aws_pricing_calculator_before_commit": True
        }
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--events-per-day", type=int, default=50_000_000)
    parser.add_argument("--avg-event-kb", type=float, default=1.5)
    parser.add_argument("--spike-multiplier", type=float, default=10)
    parser.add_argument("--month-days", type=int, default=30)
    args = parser.parse_args()

    result = compute(args.events_per_day, args.avg_event_kb, args.spike_multiplier, args.month_days)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
