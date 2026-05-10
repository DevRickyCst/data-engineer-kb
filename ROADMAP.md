# 🗺️ Roadmap — pages à ajouter

Liste des pages envisagées pour étoffer la knowledge base.
On coche au fur et à mesure. ⭐ = haute priorité (interview + quotidien).

Légende statut :
- `[ ]` à faire
- `[~]` en cours
- `[x]` fait

---

## ✅ Déjà en place

- [x] `docs/intro.md`
- [x] `docs/data-modeling/scd.md`
- [x] `docs/data-modeling/normalization.md`
- [x] `docs/data-pipeline/airflow.md`
- [x] `docs/data-pipeline/kafka.md`
- [x] `docs/interview/data-engineer.md`
- [x] `docs/interview/system-design.md`

---

## 🎯 Top 5 à attaquer en premier

Ordre recommandé, max ROI :

1. [x] ⭐ `docs/data-pipeline/dbt/fundamentals.md` — structure, materializations, tests, lineage
1. [x] ⭐ `docs/data-pipeline/dbt/advanced.md` — incremental models, snapshots, macros
2. [x] ⭐ `docs/storage/parquet-and-formats.md` — columnar, row groups, predicate pushdown, compression
3. [x] ⭐ `docs/lakehouse/{iceberg,delta-lake,hudi,table-formats-comparison}.md` — split en 4 pages dédiées
4. [x] ⭐ `docs/data-pipeline/cdc.md` — log-based vs trigger-based, Debezium, schema drift
5. [x] ⭐ `docs/quality/idempotency-and-backfills.md` — cœur du métier, déjà référencé partout

---

## 🧱 Fundamentals

- [x] ⭐ `docs/fundamentals/olap-vs-oltp.md` — workloads, storage layout, HTAP
- [x] ⭐ `docs/fundamentals/distributed-systems.md` — partitioning, replication, consensus, CAP/PACELC, consistency models
- [ ] ⭐ `docs/fundamentals/etl-vs-elt.md` — pattern moderne, cadrage du pipeline
- [x] `docs/fundamentals/batch-vs-streaming.md` — quand chacun gagne

## 📚 Data Modeling (compléter la section existante)

- [x] ⭐ `docs/data-modeling/star-vs-snowflake.md` — schémas, tradeoffs, exemples
- [ ] ⭐ `docs/data-modeling/fact-table-types.md` — transactional, periodic snapshot, accumulating snapshot, factless
- [ ] `docs/data-modeling/surrogate-vs-natural-keys.md` — court, évite des erreurs grossières
- [ ] `docs/data-modeling/one-big-table.md` — pattern moderne (BI, lakehouse)
- [ ] `docs/data-modeling/data-vault.md` — Data Vault 2.0 (optionnel, plutôt finance/assurance)
- [ ] `docs/data-modeling/bitemporal.md` — valid-time vs transaction-time (avancé)

## 💾 Storage (file formats)

- [x] ⭐ `docs/storage/parquet-and-formats.md` — Parquet, ORC, Avro
- [ ] `docs/storage/partitioning-clustering-bucketing.md` — les trois leviers
- [ ] `docs/storage/file-size-and-compaction.md` — small files problem
- [ ] `docs/storage/object-storage-basics.md` — S3/GCS, eventual consistency, listing cost

## 🏔️ Lakehouse (table formats)

- [x] ⭐ `docs/lakehouse/iceberg.md` — architecture, hidden partitioning, catalogs
- [x] ⭐ `docs/lakehouse/delta-lake.md` — transaction log, concurrency, delta-rs, UniForm
- [x] ⭐ `docs/lakehouse/hudi.md` — CoW vs MoR, indexes, timeline
- [x] ⭐ `docs/lakehouse/table-formats-comparison.md` — choix Iceberg vs Delta vs Hudi

## ⚙️ Pipelines (compléter la section)

- [x] ⭐ `docs/data-pipeline/dbt/fundamentals.md` + `docs/data-pipeline/dbt/advanced.md`
- [x] ⭐ `docs/data-pipeline/cdc.md` — Debezium, log-based vs trigger-based
- [ ] ⭐ `docs/pipeline/etl-vs-elt.md` — court mais cadre tout
- [ ] `docs/pipeline/spark-fundamentals.md` — shuffles, broadcast joins, skew
- [ ] `docs/pipeline/flink.md` — alternative streaming à Kafka Streams
- [ ] `docs/pipeline/orchestrators-comparison.md` — Airflow vs Dagster vs Prefect

## 🌊 Streaming (nouvelle section)

- [ ] ⭐ `docs/streaming/lambda-vs-kappa.md` — architectures classiques
- [ ] ⭐ `docs/streaming/event-time-watermarks.md` — la chose que tout le monde rate
- [ ] `docs/streaming/exactly-once-end-to-end.md` — vraie garantie de bout en bout
- [ ] `docs/streaming/event-sourcing-cqrs.md` — utile, plus côté backend

## ✅ Data Quality & Reliability (nouvelle section)

- [ ] ⭐ `docs/quality/data-contracts.md` — sujet chaud, "senior-flavored"
- [x] ⭐ `docs/quality/idempotency-and-backfills.md` — déjà référencé partout
- [ ] `docs/quality/testing-frameworks.md` — dbt tests vs Great Expectations vs Soda
- [ ] `docs/quality/slas-and-freshness.md` — SLA/SLO pour la data
- [ ] `docs/quality/lineage-and-observability.md` — OpenLineage, Marquez, Monte Carlo

## 🏎️ Warehouse & Performance (nouvelle section)

- [ ] ⭐ `docs/warehouse/query-optimization.md` — EXPLAIN, predicate pushdown, scan vs seek
- [ ] `docs/warehouse/cost-models.md` — BigQuery vs Snowflake vs Databricks
- [ ] `docs/warehouse/window-functions-advanced.md` — gaps & islands, dedupe, sessionization

## 🛡️ Governance & Org (plus senior)

- [ ] `docs/governance/gdpr-on-lakes.md` — right-to-erasure sur columnar
- [ ] `docs/governance/data-mesh.md` — buzzword mais demandé
- [ ] `docs/governance/reverse-etl.md` — complète le tableau

## 🎯 Interview (compléter)

- [ ] ⭐ `docs/interview/sql-patterns.md` — dedupe, top-N par groupe, gaps & islands, sessionization
- [ ] ⭐ `docs/interview/data-engineer-part2.md` — 3 nouveaux Q&A : CDC, exactly-once, "debug pipeline lent"
- [ ] `docs/interview/behavioral.md` — incidents, tradeoffs, mentoring (senior+)
- [ ] `docs/interview/system-design-cases.md` — 3-4 cas pratiques détaillés

---

## 🚀 Bonus / nice-to-have

- [ ] `docs/ml/feature-stores.md` — Feast, Tecton, online vs offline
- [ ] `docs/tools/duckdb.md` — outil qui monte, pratique en local
- [ ] `.github/workflows/deploy.yml` — déploiement S3 + CloudFront
- [ ] Plugin de **search local** (`@easyops-cn/docusaurus-search-local`)
- [ ] Page `/about` ou `/contact`

## 🧰 Tooling / DX

- [x] Glossary system with hover tooltips (`<T>` MDX component, `src/glossary.js`, `docs/glossary.md`)
- [ ] Auto-generate `docs/glossary.md` from `src/glossary.js` (pre-build script) — defer until drift becomes a problem
- [ ] CI check that every `src/glossary.js` key has a matching anchor in `docs/glossary.md`
- [ ] Roll out `<T>` wrapping across all existing pages (currently only on `distributed-systems.md` and `batch-vs-streaming.md`)

---

## 📋 Notes

- Garder le format des pages cohérent : intro courte → concepts → exemples (SQL/code/Mermaid) → pros/cons → pitfalls → follow-up
- Toujours inclure au moins un diagramme Mermaid quand ça aide
- Les exemples SQL sont en ANSI sauf mention contraire
- Les Q&A interview gardent le format Junior / Mid / Senior + Pitfalls + Follow-ups
