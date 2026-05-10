---
id: glossary
title: Glossary
sidebar_label: Glossary
description: Definitions of the recurring distributed systems and data engineering terms used across this knowledge base.
---

# Glossary

Single reference for the jargon-heavy terms that recur across this knowledge base. Each entry has a short definition and one or two pages where the term is most relevant.

Pages link here via the `<T>` component — hovering an underlined term shows a quick definition; clicking jumps to the entry below. To add a term, edit `src/glossary.js` and add a matching `## Display name {#slug}` section here with the same key.

---

## BCNF (Boyce-Codd Normal Form) {#bcnf}

A stricter version of 3NF where every non-trivial functional dependency has a superkey on the left side. Only differs from 3NF when a table has overlapping candidate keys; in everyday OLTP design, 3NF is enough.

**Used on:** [Normalization](./data-modeling/normalization).

---

## CAP theorem {#cap}

Brewer's theorem (formalized by Gilbert & Lynch 2002): during a network partition, a distributed system must choose between consistency (refuse possibly-stale data) and availability (serve it anyway). Partition tolerance is not optional — partitions happen in real networks.

**Used on:** [Distributed Systems](./fundamentals/distributed-systems).

---

## CDC (Change Data Capture) {#cdc}

A pattern for streaming row-level changes (insert/update/delete) from a transactional database to downstream systems. Log-based CDC reads the WAL/binlog directly (Debezium, Postgres logical replication); trigger-based CDC fires on writes.

**Used on:** [CDC](./data-pipeline/cdc), [OLAP vs OLTP](./fundamentals/olap-vs-oltp).

---

## Checkpoint {#checkpoint}

A periodic snapshot of a streaming operator's state to durable storage. On failure, the operator restarts from the last checkpoint and replays events since then. Flink uses Chandy-Lamport asynchronous snapshots; Kafka Streams uses changelog topics.

**Used on:** [Batch vs Streaming](./fundamentals/batch-vs-streaming), [Distributed Systems](./fundamentals/distributed-systems).

---

## Consensus {#consensus}

A distributed protocol that gets a group of nodes to agree on a single value (or a sequence of values) despite failures and message loss. Used to elect leaders, commit transactions, and order operations. Paxos and Raft are the canonical algorithms.

**Used on:** [Distributed Systems](./fundamentals/distributed-systems).

---

## Eventual consistency {#eventual-consistency}

A consistency model where replicas converge eventually, with no ordering guarantees in between. The default for AP systems like Cassandra and DynamoDB. Concrete impact: a write may not be visible to other clients immediately, and concurrent writes may need application-level reconciliation.

**Used on:** [Distributed Systems](./fundamentals/distributed-systems).

---

## Exactly-once {#exactly-once}

A processing guarantee that the *effect* of each event happens exactly once, even when the underlying delivery is retried. End-to-end exactly-once requires at-least-once delivery plus idempotent sinks — pure "exactly-once delivery" is impossible in an asynchronous network.

**Used on:** [Distributed Systems](./fundamentals/distributed-systems), [Kafka](./data-pipeline/kafka), [CDC](./data-pipeline/cdc), [Idempotency & Backfills](./quality/idempotency-and-backfills).

---

## FLP impossibility {#flp}

Fischer, Lynch, Paterson (1985): in a fully asynchronous network with even one faulty process, no deterministic consensus algorithm can guarantee termination. Real systems sidestep this with timeouts and randomized backoff — which is why Raft elections feel slow under flaky networks.

**Used on:** [Distributed Systems](./fundamentals/distributed-systems).

---

## Idempotency {#idempotency}

A property where applying an operation multiple times yields the same result as applying it once. The cornerstone of safe retries in distributed systems and the prerequisite for exactly-once semantics.

**Used on:** [Idempotency & Backfills](./quality/idempotency-and-backfills) (dedicated page).

---

## ISR (In-Sync Replicas) {#isr}

Kafka replicas that have fully caught up with the partition leader. Producers configured with `acks=all` only get acknowledged once all ISR have written, and `min.insync.replicas` controls how many are required for a write to succeed.

**Used on:** [Kafka](./data-pipeline/kafka), [Distributed Systems](./fundamentals/distributed-systems).

---

## Linearizable {#linearizable}

The strongest single-object consistency model: every operation appears to take effect at a single point in time, and every observer sees the same total order. Spanner is linearizable globally via TrueTime; single-node Postgres is linearizable per row.

**Used on:** [Distributed Systems](./fundamentals/distributed-systems).

---

## MVCC (Multi-Version Concurrency Control) {#mvcc}

Each write creates a new version of the row instead of overwriting in place. Readers see a consistent snapshot without taking locks, and writers do not block readers. Used in Postgres, Snowflake, Iceberg, and most modern OLTP/OLAP engines.

**Used on:** [OLAP vs OLTP](./fundamentals/olap-vs-oltp), [Iceberg](./lakehouse/iceberg).

---

## OBT (One Big Table) {#obt}

A modeling pattern where every column the consumer might need is denormalized into a single wide table. Common in BI semantic layers (Looker, Tableau) and modern lakehouses where columnar storage makes wide tables cheap to scan.

**Used on:** [Normalization](./data-modeling/normalization), [Star vs Snowflake](./data-modeling/star-vs-snowflake).

---

## PACELC {#pacelc}

Refinement of CAP by Daniel Abadi (2010): if Partitioned, choose Availability or Consistency; Else (no partition), choose Latency or Consistency. Captures the trade-off you live with day-to-day on every quorum read, not just during rare partitions.

**Used on:** [Distributed Systems](./fundamentals/distributed-systems).

---

## Paxos {#paxos}

Lamport's consensus algorithm (1989), the original. Conceptually elegant but notoriously hard to implement correctly. Used in Google Spanner (Multi-Paxos), Chubby, and several internal Google systems.

**Used on:** [Distributed Systems](./fundamentals/distributed-systems).

---

## Predicate pushdown {#predicate-pushdown}

An optimization where filters (`WHERE` clauses) are pushed as close to the storage layer as possible, so the engine reads less data. Critical for columnar formats like Parquet — saves 10-100× I/O on selective queries by skipping row groups via min/max stats.

**Used on:** [Parquet & file formats](./storage/parquet-and-formats).

---

## Quorum {#quorum}

A majority of nodes (typically ⌊N/2⌋+1) that must agree before a write or read is considered authoritative. A 3-node cluster needs 2; a 5-node cluster needs 3. Even-numbered clusters waste a node — they tolerate the same number of failures as the next-lower odd size.

**Used on:** [Distributed Systems](./fundamentals/distributed-systems).

---

## Raft {#raft}

A consensus algorithm by Ongaro & Ousterhout (2014), designed to be more understandable than Paxos while providing the same guarantees. Used in etcd, Consul, CockroachDB, TiKV, and Kafka KRaft mode (the ZooKeeper replacement, default since 3.3).

**Used on:** [Distributed Systems](./fundamentals/distributed-systems).

---

## RPO (Recovery Point Objective) {#rpo}

The maximum tolerable amount of data loss, measured in time. An RPO of 5 minutes means after a disaster, the system can lose up to 5 minutes of recent writes. Drives backup frequency and replication strategy.

**Used on:** [Idempotency & Backfills](./quality/idempotency-and-backfills).

---

## RTO (Recovery Time Objective) {#rto}

The maximum tolerable downtime after a disaster before the system is back up. An RTO of 1 hour means recovery procedures must complete within an hour. Drives failover automation and runbook design.

**Used on:** [Idempotency & Backfills](./quality/idempotency-and-backfills).

---

## SCD (Slowly Changing Dimension) {#scd}

Kimball's pattern for tracking how dimension attributes change over time. Type-1 overwrites; Type-2 keeps a full history with `valid_from`/`valid_to` columns; Type-3 keeps the previous value alongside the current. Type-2 is the most common in analytics.

**Used on:** [SCD](./data-modeling/scd) (dedicated page).

---

## Shuffle {#shuffle}

Cross-node data redistribution between stages of a distributed query. Required when a join or aggregation key is not co-located. Usually the dominant cost in MPP query plans; skewed shuffle (one partition far larger than others) is the #1 cause of slow Spark jobs.

**Used on:** [Batch vs Streaming](./fundamentals/batch-vs-streaming), [Distributed Systems](./fundamentals/distributed-systems).

---

## Vectorized execution {#vectorized}

Execution model that processes batches of rows (typically 1024 at a time) through pipelined operators using SIMD. The dominant model in modern OLAP engines (DuckDB, ClickHouse, Snowflake), 10-100× faster than tuple-at-a-time execution for scans and aggregations.

**Used on:** [OLAP vs OLTP](./fundamentals/olap-vs-oltp), [Parquet & file formats](./storage/parquet-and-formats).

---

## WAL (Write-Ahead Log) {#wal}

Every change is durably appended to a sequential log before being applied to the table. Enables crash recovery, replication, and CDC. Postgres has the WAL, MySQL has the binlog, Kafka itself is a WAL.

**Used on:** [CDC](./data-pipeline/cdc), [Iceberg](./lakehouse/iceberg).

---

## Watermark {#watermark}

A logical timestamp in stream processing that signals "no events with timestamp `< T` will arrive anymore". Engines use it to decide when a window can close and emit a result. Setting it too aggressively drops late events; too laxly adds latency.

**Used on:** [Batch vs Streaming](./fundamentals/batch-vs-streaming), [Kafka](./data-pipeline/kafka).
