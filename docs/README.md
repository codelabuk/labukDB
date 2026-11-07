# Flow charts — index

Architecture diagrams for labukDB, as [Mermaid](https://mermaid.js.org/)
diagrams embedded directly in each `.md` file below. GitHub renders
Mermaid natively, inline, in any Markdown file preview — no plugin, no
external renderer, no exported image to keep in sync. These used to be
PlantUML (`.puml`) sources with no GitHub-native way to view them;
converted to close that gap for good, not just work around it (a prior
attempt to render PlantUML via the public plantuml.com web service was
also blocked by this environment's own permission system, as a
data-sharing action — Mermaid needs no such external call at all).

## Current diagrams

| File | Covers |
|---|---|
| [`module-overview.md`](flow-charts/module-overview.md) | The five Maven modules, their dependency direction, and the one-way `core` ↛ `format-iceberg` rule that shapes where format detection vs. format construction live. |
| [`delta-write-path.md`](flow-charts/delta-write-path.md) | `log/`'s write path — `TableCreator`/`CommitWriter`/`TableMerger`/`Compactor`/`Vacuum`, the confirmed-broken-without-lock finding, and `DynamoDbLockProvider`'s fix. |
| [`iceberg-write-path.md`](flow-charts/iceberg-write-path.md) | `labukdb-format-iceberg`'s write path — same shape as Delta's, plus the REST catalog + real MinIO full data round-trip proof and the two live-only issues found fixing it. |
| [`read-path-pruning-decode.md`](flow-charts/read-path-pruning-decode.md) | `planner/` (the four pruning levels) + `decode/` (Parquet → Arrow, nested types, decimal, timestamps). |
| [`sql-engine-logical-plan.md`](flow-charts/sql-engine-logical-plan.md) | **SQL engine architecture** — the `core.logical` IR + optimizer, the before/after tree-shape transformation, joins, and where the Phase 1–4 arc actually stands today. |
| [`flight-query-sequence.md`](flow-charts/flight-query-sequence.md) | The Flight `GetFlightInfo`/`DoGet` round trip, with current class names. |
| [`catalog-and-format-detection.md`](flow-charts/catalog-and-format-detection.md) | How a raw `--table` arg, a SQL `FROM`-clause name, or a Flight request's `table` field resolves all the way to an opened, format-detected table. |

No legacy diagrams kept — the old single mega-diagram and the two v1
diagrams it and this folder used to carry forward (`architecture.puml`,
`flight-sequence.puml`) were already self-labeled superseded/historical
before this conversion; deleted rather than converted, since git history
preserves them if ever needed again.
