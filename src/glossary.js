/**
 * Single source of truth for glossary terms.
 *
 * Keys are URL-safe slugs that match:
 *   1. The lookup key passed to <T term="..."> (or matched case-insensitively
 *      from the children text when omitted).
 *   2. The anchor `## Term {#slug}` in `docs/glossary.md`.
 *
 * Definitions are plain text (no markdown). The tooltip and the glossary page
 * both render them verbatim. Cross-links live on the glossary page itself.
 *
 * To add a term:
 *   1. Add an entry below.
 *   2. Add a matching `## Display Name {#key}` heading + paragraph in docs/glossary.md.
 *   3. Wrap the first occurrence per page with <T>term</T>.
 */
const glossary = {
  quorum: {
    term: 'Quorum',
    definition:
      'A majority of nodes (typically ⌊N/2⌋+1) that must agree before a write or read is considered authoritative. A 3-node cluster needs 2; a 5-node cluster needs 3. Even-numbered clusters waste a node — they tolerate the same number of failures as the next-lower odd size.',
  },
  isr: {
    term: 'ISR (In-Sync Replicas)',
    definition:
      'Kafka replicas that have fully caught up with the partition leader. Producers configured with acks=all only get acknowledged once all ISR have written, and min.insync.replicas controls how many are required for a write to succeed.',
  },
  watermark: {
    term: 'Watermark',
    definition:
      'A logical timestamp in stream processing that signals "no events with timestamp < T will arrive anymore". Engines use it to decide when a window can close and emit a result. Setting it too aggressively drops late events; too laxly adds latency.',
  },
  mvcc: {
    term: 'MVCC (Multi-Version Concurrency Control)',
    definition:
      'Each write creates a new version of the row instead of overwriting in place. Readers see a consistent snapshot without taking locks, and writers do not block readers. Used in Postgres, Snowflake, Iceberg, and most modern OLTP/OLAP engines.',
  },
  cap: {
    term: 'CAP theorem',
    definition:
      "Brewer's theorem (formalized by Gilbert & Lynch 2002): during a network partition, a distributed system must choose between consistency (refuse possibly-stale data) and availability (serve it anyway). Partition tolerance is not optional — partitions happen in real networks.",
  },
  pacelc: {
    term: 'PACELC',
    definition:
      'Refinement of CAP by Daniel Abadi (2010): if Partitioned, choose Availability or Consistency; Else (no partition), choose Latency or Consistency. Captures the trade-off you live with day-to-day on every quorum read, not just during rare partitions.',
  },
  bcnf: {
    term: 'BCNF (Boyce-Codd Normal Form)',
    definition:
      'A stricter version of 3NF where every non-trivial functional dependency has a superkey on the left side. Only differs from 3NF when a table has overlapping candidate keys; in everyday OLTP design, 3NF is enough.',
  },
  flp: {
    term: 'FLP impossibility',
    definition:
      'Fischer, Lynch, Paterson (1985): in a fully asynchronous network with even one faulty process, no deterministic consensus algorithm can guarantee termination. Real systems sidestep this with timeouts and randomized backoff — which is why Raft elections feel slow under flaky networks.',
  },
  rpo: {
    term: 'RPO (Recovery Point Objective)',
    definition:
      'The maximum tolerable amount of data loss, measured in time. An RPO of 5 minutes means after a disaster, the system can lose up to 5 minutes of recent writes. Drives backup frequency and replication strategy.',
  },
  rto: {
    term: 'RTO (Recovery Time Objective)',
    definition:
      'The maximum tolerable downtime after a disaster before the system is back up. An RTO of 1 hour means recovery procedures must complete within an hour. Drives failover automation and runbook design.',
  },
  'exactly-once': {
    term: 'Exactly-once',
    definition:
      "A processing guarantee that the *effect* of each event happens exactly once, even when the underlying delivery is retried. End-to-end exactly-once requires at-least-once delivery plus idempotent sinks — pure 'exactly-once delivery' is impossible in an asynchronous network.",
  },
  wal: {
    term: 'WAL (Write-Ahead Log)',
    definition:
      'Every change is durably appended to a sequential log before being applied to the table. Enables crash recovery, replication, and CDC. Postgres has the WAL, MySQL has the binlog, Kafka itself is a WAL.',
  },
  'predicate-pushdown': {
    term: 'Predicate pushdown',
    definition:
      'An optimization where filters (WHERE clauses) are pushed as close to the storage layer as possible, so the engine reads less data. Critical for columnar formats like Parquet — saves 10-100× I/O on selective queries by skipping row groups via min/max stats.',
  },
  vectorized: {
    term: 'Vectorized execution',
    definition:
      'Execution model that processes batches of rows (typically 1024 at a time) through pipelined operators using SIMD. The dominant model in modern OLAP engines (DuckDB, ClickHouse, Snowflake), 10-100× faster than tuple-at-a-time execution for scans and aggregations.',
  },
  shuffle: {
    term: 'Shuffle',
    definition:
      'Cross-node data redistribution between stages of a distributed query. Required when a join or aggregation key is not co-located. Usually the dominant cost in MPP query plans; skewed shuffle (one partition far larger than others) is the #1 cause of slow Spark jobs.',
  },
  checkpoint: {
    term: 'Checkpoint',
    definition:
      "A periodic snapshot of a streaming operator's state to durable storage. On failure, the operator restarts from the last checkpoint and replays events since then. Flink uses Chandy-Lamport asynchronous snapshots; Kafka Streams uses changelog topics.",
  },
  consensus: {
    term: 'Consensus',
    definition:
      'A distributed protocol that gets a group of nodes to agree on a single value (or a sequence of values) despite failures and message loss. Used to elect leaders, commit transactions, and order operations. Paxos and Raft are the canonical algorithms.',
  },
  raft: {
    term: 'Raft',
    definition:
      'A consensus algorithm by Ongaro & Ousterhout (2014), designed to be more understandable than Paxos while providing the same guarantees. Used in etcd, Consul, CockroachDB, TiKV, and Kafka KRaft mode (the ZooKeeper replacement, default since 3.3).',
  },
  paxos: {
    term: 'Paxos',
    definition:
      "Lamport's consensus algorithm (1989), the original. Conceptually elegant but notoriously hard to implement correctly. Used in Google Spanner (Multi-Paxos), Chubby, and several internal Google systems.",
  },
  linearizable: {
    term: 'Linearizable',
    definition:
      'The strongest single-object consistency model: every operation appears to take effect at a single point in time, and every observer sees the same total order. Spanner is linearizable globally via TrueTime; single-node Postgres is linearizable per row.',
  },
  'eventual-consistency': {
    term: 'Eventual consistency',
    definition:
      'A consistency model where replicas converge eventually, with no ordering guarantees in between. The default for AP systems like Cassandra and DynamoDB. Concrete impact: a write may not be visible to other clients immediately, and concurrent writes may need application-level reconciliation.',
  },
  idempotency: {
    term: 'Idempotency',
    definition:
      'A property where applying an operation multiple times yields the same result as applying it once. The cornerstone of safe retries in distributed systems and the prerequisite for exactly-once semantics.',
  },
  cdc: {
    term: 'CDC (Change Data Capture)',
    definition:
      'A pattern for streaming row-level changes (insert/update/delete) from a transactional database to downstream systems. Log-based CDC reads the WAL/binlog directly (Debezium, Postgres logical replication); trigger-based CDC fires on writes.',
  },
  scd: {
    term: 'SCD (Slowly Changing Dimension)',
    definition:
      "Kimball's pattern for tracking how dimension attributes change over time. Type-1 overwrites; Type-2 keeps a full history with valid_from/valid_to columns; Type-3 keeps the previous value alongside the current. Type-2 is the most common in analytics.",
  },
  obt: {
    term: 'OBT (One Big Table)',
    definition:
      'A modeling pattern where every column the consumer might need is denormalized into a single wide table. Common in BI semantic layers (Looker, Tableau) and modern lakehouses where columnar storage makes wide tables cheap to scan.',
  },
};

module.exports = glossary;
