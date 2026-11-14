# Flow charts — index

Architecture diagrams for labukDB, as [Mermaid](https://mermaid.js.org/)
diagrams embedded directly in each `.md` file below. 

## Current diagrams

Start with `full-architecture.md` for the single whole-system picture; everything else here zooms into one slice of it.

| File | Covers |
|---|---|
| [`full-architecture.md`](diagrams/full-architecture.md) | **The whole system, one diagram** — every layer (entry points, SQL engine, format abstraction, both write engines, planner/pruning, decode/encode, execution operators, storage) tied together, write path and read/query path both. |
| [`module-overview.md`](diagrams/module-overview.md) | The five Maven modules, their dependency direction, and the one-way `core` ↛ `format-iceberg` rule that shapes where format detection vs. format construction live. |
| [`delta-write-path.md`](diagrams/delta-write-path.md) | `log/`'s write path — `TableCreator`/`CommitWriter`/`TableMerger`/`Compactor`/`Vacuum`, the confirmed-broken-without-lock finding, and `DynamoDbLockProvider`'s fix. |
| [`iceberg-write-path.md`](diagrams/iceberg-write-path.md) | `labukdb-format-iceberg`'s write path — same shape as Delta's, plus the REST catalog + real MinIO full data round-trip proof and the two live-only issues found fixing it. |
| [`read-path-pruning-decode.md`](diagrams/read-path-pruning-decode.md) | `planner/` (the four pruning levels) + `decode/` (Parquet → Arrow, nested types, decimal, timestamps). |
| [`sql-engine-logical-plan.md`](diagrams/sql-engine-logical-plan.md) | **SQL engine architecture** — the `core.logical` IR + optimizer, the before/after tree-shape transformation, joins, and where the Phase 1–4 arc actually stands today. |
| [`flight-query-sequence.md`](diagrams/flight-query-sequence.md) | The Flight `GetFlightInfo`/`DoGet` round trip, with current class names. |
| [`catalog-and-format-detection.md`](diagrams/catalog-and-format-detection.md) | How a raw `--table` arg, a SQL `FROM`-clause name, or a Flight request's `table` field resolves all the way to an opened, format-detected table. |
