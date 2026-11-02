# Flow charts — index

PlantUML (`.puml`) sources for labukDB's architecture. All diagrams in
this folder use plain component/sequence syntax so they render in any
PlantUML viewer (VS Code's PlantUML extension, `plantuml.jar`, the
plantuml.com online editor, etc.) — paste the `.puml` file's contents in.

**Known limitation, stated plainly:** this session could not regenerate
`.svg`/`.png`/`.pdf` renders for any of the files below — uploading to
the public PlantUML web server is blocked by this environment's
permission system as a data-sharing action. Only
`labukdb-v2-module-architecture.puml` has pre-existing renders (`.svg`/
`.png`/`.pdf`), and those now predate this session's patches to the
source — render them yourself locally if you need an up-to-date image.

## Current, per-topic diagrams (new this session)

| File | Covers |
|---|---|
| [`module-overview.puml`](flow-charts/module-overview.puml) | The five Maven modules, their dependency direction, and the one-way `core` ↛ `format-iceberg` rule that shapes where format detection vs. format construction live. |
| [`delta-write-path.puml`](flow-charts/delta-write-path.puml) | `log/`'s write path — `TableCreator`/`CommitWriter`/`TableMerger`/`Compactor`/`Vacuum`, the confirmed-broken-without-lock finding, and `DynamoDbLockProvider`'s fix. |
| [`iceberg-write-path.puml`](flow-charts/iceberg-write-path.puml) | `labukdb-format-iceberg`'s write path — same shape as Delta's, plus the REST catalog + real MinIO full data round-trip proof and the two live-only issues found fixing it. |
| [`read-path-pruning-decode.puml`](flow-charts/read-path-pruning-decode.puml) | `planner/` (the four pruning levels) + `decode/` (Parquet → Arrow, nested types, decimal, timestamps). |
| [`sql-engine-logical-plan.puml`](flow-charts/sql-engine-logical-plan.puml) | **SQL engine architecture, Phase 1** — the new `core.logical` IR + optimizer shell, the actual before/after tree-shape transformation, and the Phase 2–4 arc beyond it. |
| [`flight-query-sequence.puml`](flow-charts/flight-query-sequence.puml) | The Flight `GetFlightInfo`/`DoGet` round trip, rebuilt with current class names (replaces the stale legacy sequence diagram below). |
| [`catalog-and-format-detection.puml`](flow-charts/catalog-and-format-detection.puml) | How a raw `--table` arg, a SQL `FROM`-clause name, or a Flight request's `table` field resolves all the way to an opened, format-detected table. |

## Legacy diagrams (moved here, not current)

| File | Status |
|---|---|
| [`labukdb-v2-module-architecture.puml`](labukdb-v2-module-architecture.puml) (+ `.svg`/`.png`/`.pdf`) | The one mega-diagram every v2 milestone used to update in place. Source **patched** this session for the most actively-wrong facts (the `DeltaTableFormat`/CLI-dispatch notes, `NestedTypeDecoder`'s scope), but **not re-rendered** (see limitation above) and **not** given the new `core.logical` detail — prefer `sql-engine-logical-plan.puml` and the other per-topic diagrams above for anything current. Superseded in spirit by this folder's per-topic split, kept for history. |
| [`architecture.puml`](flow-charts/architecture.puml) | v1 system diagram, already self-labeled "superseded" before this folder existed. Historical only. |
| [`flight-sequence.puml`](flow-charts/flight-sequence.puml) | v1 sequence diagram — references `DeltaFlightProducer`/`DeltaCli`, classes renamed to `LabukDbFlightProducer`/`LabukDbCli` long ago. Superseded by `flight-query-sequence.puml` above. Kept for history, not fixed in place.
