---
id: data-engineer
title: Data Engineer Interview — Q&A
sidebar_label: Data Engineer
description: Real interview questions answered at Junior, Mid, and Senior levels — with pitfalls and follow-ups.
---

# Data Engineer Interview — Q&A

Each question is answered at three levels:

- **Junior** — what a strong candidate with 0–2 years would say.
- **Mid-level** — adds tradeoffs, operational awareness, and tooling.
- **Senior** — adds system design, organizational tradeoffs, and "what could go wrong at scale."

You can read these top-down to study, or pick a level and rehearse out loud.

---

## Question 1 — "Explain Slowly Changing Dimensions. When would you use Type 2 over Type 1?"

### Answer — Junior

> SCDs are how dimension tables handle changes over time.
>
> - **Type 1** overwrites the value — you lose the old one.
> - **Type 2** keeps history by inserting a new row with `valid_from` / `valid_to` columns and an `is_current` flag.
> - **Type 3** keeps the previous value in an extra column.
>
> Use **Type 2** when you need to know "what was true at the time of the event" — for example, the customer's city when an order was placed. Use **Type 1** for things like fixing a typo.

### Answer — Mid-level

> The choice is driven by what historical reports need to look like.
>
> Type 2 is the default for any attribute that joins to a fact table when historical accuracy matters — region, segment, manager. The fact table joins on the **surrogate key**, not the natural key, so a query for 2023 sales by region uses the dimension version that was current in 2023.
>
> Implementation-wise I'd use `dbt snapshots` or a warehouse-native `MERGE` to close the active row (`valid_to = today - 1`, `is_current = false`) and insert the new one. The `is_current` flag is just an index optimization — the source of truth is the date range.
>
> Type 1 I keep for corrections (typos, bad codes) or attributes with no analytical history value (e.g., contact email).

### Answer — Senior

> The real conversation isn't "which SCD type" — it's **per-column policy** and **late-arriving facts**.
>
> A single dimension usually mixes types: `email` is Type 1, `region` is Type 2, `risk_segment` might be Type 3 because product only ever wants old-vs-new. Some shops codify this with a "Type 6" hybrid (current + historical columns + a history table) but I find that brittle — I'd rather have explicit per-column rules in the dbt project and a CI check that flags unintended Type-1 changes on Type-2 columns.
>
> The harder problem is **late-arriving facts**: a transaction that arrives 3 months late must join to the dimension version that was current *at the event timestamp*, not today. That requires range joins (`event_ts BETWEEN valid_from AND valid_to`) which most BI tools generate poorly. I usually pre-resolve `dim_sk` at ingest time so the fact carries the right surrogate key from the start.
>
> Operationally I also watch for: surrogate-key collisions across environments, `is_current` index bloat, and what happens during a backfill — re-running a snapshot for an old date with today's source data corrupts history. Snapshots must be append-only and never re-run for historical dates.

### Common pitfalls

- Joining facts on the **natural key** (`customer_id`) instead of the **surrogate key** — silently breaks historical reports.
- Using `valid_to = NULL` instead of a sentinel `9999-12-31`. Breaks `BETWEEN` joins.
- Re-running a Type-2 snapshot for a historical date — corrupts the history with present-day data.

### Follow-up questions

- How would you handle a column that is Type 1 today but stakeholders later want Type 2?
- How do you resolve `dim_sk` for a fact that arrives 6 months late?
- What's the cost of `is_current = true` filter at 100M dimension rows? How would you optimize it?

---

## Question 2 — "How do you choose a partitioning strategy for a large warehouse fact table?"

### Answer — Junior

> Partitioning splits a table into smaller chunks so queries can skip the parts they don't need.
>
> The most common choice is partitioning by **date** — usually the event date — because most queries filter by time ("last 7 days", "last month"). The query engine reads only the relevant partitions, which is much faster.
>
> I'd partition by day for high-volume tables and by month for smaller ones, to avoid having too many tiny partitions.

### Answer — Mid-level

> The strategy depends on three things: query patterns, data volume, and the engine.
>
> For most fact tables I partition by **event date** (daily) because:
> - Queries almost always have a date range filter.
> - Backfills and reprocessing target specific date ranges.
> - Late-arriving data lands in known partitions.
>
> Sub-partitioning or **clustering** comes second. In BigQuery I'd cluster by the highest-cardinality filter column (e.g., `customer_id`); in Snowflake I'd let auto-clustering handle it; in Spark/Iceberg I'd use bucketed columns or `Z-ordering`.
>
> Things I avoid:
> - Partitioning by a column with **too few distinct values** (e.g., country with 5 countries) — partitions become too big to help.
> - Partitioning by a column with **too many distinct values** (e.g., user_id) — millions of tiny partitions wreck the metadata layer.
> - Partitioning by `ingestion_date` when users always filter on `event_date` — you'll scan the whole table anyway.

### Answer — Senior

> Partitioning is one lever — I think about it as part of a layout strategy that includes file size, clustering, and compaction.
>
> Defaults I start from:
> - **Partition by event date**, daily.
> - **Cluster** by 1–2 high-selectivity columns that appear in `WHERE`/`JOIN`.
> - **Target file size** ~256MB–1GB (engine-dependent). Many small files = metadata explosion; few huge files = no parallelism.
>
> Then I look at what breaks at scale:
> - **Skew.** A "country" partition where 70% of events are US wrecks parallelism. Fix with secondary key in the partition spec or salting.
> - **Late-arriving data.** Daily partitions get touched repeatedly for days. With Iceberg/Delta this is fine; with Hive-style partitions it forces re-writes. Compaction strategy must account for it.
> - **Schema evolution.** Partition keys are essentially baked into storage; changing them later is a rewrite. I pick conservatively.
> - **Cost model of the engine.** BigQuery charges by bytes scanned, so partitioning is a cost issue, not just a perf issue. Snowflake charges by warehouse time, so good clustering matters more than partitioning.
>
> One bias I've developed: I'd rather **over-partition by time and under-partition by everything else**. Time partitioning is universal, easy to reason about, and aligns with how the rest of the org thinks (backfills, retention, GDPR deletion). Adding a clever secondary partition for a single query pattern is a trap — query patterns drift faster than table layouts.

### Common pitfalls

- Partitioning by `ingestion_date` instead of `event_date` — users filter by event date, so partitions don't help.
- Too many tiny partitions → metadata layer (Hive Metastore, Glue, etc.) becomes the bottleneck before the data does.
- Forgetting that **partition pruning only happens with literal/static filters**. `WHERE event_date >= (SELECT MAX(...) FROM other_table)` often disables pruning.

### Follow-up questions

- A query is supposed to read 1 partition but reads all of them. How do you debug it?
- How does partitioning differ between Hive, Iceberg, Delta, and a cloud warehouse like BigQuery?
- How would you handle GDPR right-to-be-forgotten on a partitioned, columnar table?

---

## Question 3 — "Walk me through how you'd design a daily pipeline that ingests order events from a transactional DB into a warehouse."

### Answer — Junior

> I'd extract the data once a day, transform it, and load it into the warehouse.
>
> 1. Use Airflow to schedule a daily DAG.
> 2. Extract from the source DB — for example with a `SELECT * FROM orders WHERE updated_at >= ...`.
> 3. Land the raw data in a staging area (S3 or a `raw_` schema).
> 4. Transform with SQL (or dbt) — clean, deduplicate, join with dimensions.
> 5. Write to a `marts` schema with a partition by date.
> 6. Add a quality check (row count, no nulls in PK) and alert if it fails.

### Answer — Mid-level

> I'd start by clarifying a few requirements: latency (daily? hourly?), volume, schema stability, and whether deletes/updates need to be tracked. Assuming daily and ~10M rows/day:
>
> 1. **Extract.** Use **CDC if available** (Debezium → Kafka, or the warehouse's native connector). If not, use **incremental SELECT on `updated_at`** with a small overlap window (e.g., 30 min) to handle clock skew. Avoid `SELECT *` from the prod replica during peak hours.
> 2. **Land.** Write raw, append-only files to S3 in a date-partitioned path: `s3://lake/raw/orders/event_date=YYYY-MM-DD/`. Format: Parquet, one file per task instance.
> 3. **Stage.** External table or `COPY INTO` a `staging.orders` table in the warehouse. Keep it <T term="idempotency">idempotent</T> — `MERGE` keyed on `(order_id, source_updated_at)`.
> 4. **Transform.** dbt models: `stg_orders` → `int_orders_enriched` (join dims) → `fct_orders`. Keep `stg_` 1:1 with source for traceability.
> 5. **Quality.** dbt tests (`unique`, `not_null`, `relationships`), plus a freshness check on `max(updated_at)` and a row-count anomaly check vs. last 7 days.
> 6. **Orchestrate.** One Airflow DAG, idempotent on `logical_date`. Partition pruning everywhere. Failures alert to the on-call channel.
>
> I'd document the SLA (e.g., "fct_orders ready by 07:00 UTC") and the recovery path (rerun for `logical_date=X`).

### Answer — Senior

> I want to push back on the question first: **do we need a daily pipeline, or are we defaulting to it?** If the consumers are dashboards refreshed once a day, daily is fine. If anyone downstream is making operational decisions, daily is a six-hour stale dataset and we'll regret it. Let's say daily is right.
>
> Architecture I'd land on:
>
> ```
> Source DB → Debezium CDC → Kafka (orders.cdc) → S3 raw (Parquet)
>                                                    ↓
>                                      Iceberg staging (incremental MERGE)
>                                                    ↓
>                                       dbt marts (fct_orders, dim_*)
>                                                    ↓
>                                       Quality gate (Great Expectations / dbt)
>                                                    ↓
>                                          Consumers (BI, ML, reverse-ETL)
> ```
>
> Why each piece:
>
> - **CDC over batch SELECT.** Hard deletes get captured. No load on the prod DB. Schema changes are visible in the CDC stream. The cost is operational complexity (Debezium + Schema Registry).
> - **Kafka as a buffer.** Decouples source DB hiccups from warehouse load. Lets us replay if the warehouse-side logic has a bug.
> - **Iceberg/Delta** instead of plain Parquet. Schema evolution, time-travel, ACID merges. GDPR deletes become tractable.
> - **dbt for transforms.** Versioned, tested, lineage-aware. The marts layer is what stakeholders see.
> - **Quality gate as a hard stop.** A failing test must block downstream consumption — write to a `_pending` table, only swap to `published` after green tests. "Soft" alerts get ignored.
>
> Things that bite at scale:
>
> - **Schema drift.** Source DB adds a column, CDC propagates a schema change, dbt blows up. Solution: schema registry + compatibility checks in CI, and `staging` models that explicitly cast the columns we care about.
> - **Late-arriving events.** A 3-day-old order update lands in today's batch. The fact table needs the right `dim_sk` for the event time, not today. Pre-resolve dimension keys at ingest.
> - **Backfills.** Re-running 6 months of data with today's transformation logic must produce the same output. Means: deterministic SQL (no `now()`), reproducible inputs (CDC log retention or an immutable raw layer).
> - **Cost.** Naive `MERGE` on a 1B-row fact table every day is expensive. Partition pruning + clustered keys + writing only the affected partitions matters.
> - **Ownership.** Who's paged at 03:00 when the pipeline fails? I bake that into the design — the team that owns the fact table owns the on-call.
>
> If I had to ship a v1 in a week, I'd skip CDC and Iceberg and use incremental dbt models on a warehouse-native connector. The architecture above is what I'd plan toward, not what I'd start with.

### Common pitfalls

- Using `SELECT * FROM orders WHERE updated_at >= last_run` without an overlap → drops rows on clock skew.
- Soft-deleting in the source but not capturing it → orphan rows in the warehouse forever.
- Putting business logic in Airflow `PythonOperator`s instead of SQL/dbt → no lineage, no tests, hard to audit.
- "We'll add data quality later" → there is no later.
- Skipping idempotency, then realizing a backfill double-counts revenue. (Long-form: [Idempotency & Backfills](../quality/idempotency-and-backfills).)

### Follow-up questions

- The source DB doesn't expose `updated_at` and you can't add CDC. What now?
- A bug in the transform layer corrupted 3 months of `fct_orders`. Walk me through the recovery.
- The pipeline's runtime has been growing 5%/week. How do you investigate?
- How does this design change if the SLA goes from daily to 5 minutes?
