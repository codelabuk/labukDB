# labukDB v2 — Multi-Module Architecture

**Status: planned, not implemented.** This document captures the agreed
design for turning `delta-flight-engine` (a read-only Delta Lake gateway)
into **labukDB** — a multi-format lakehouse database that reads and writes
Delta tables, reads Iceberg tables, and is usable either as an embedded
Java library or as a Flight server. Nothing here exists in code yet; see
[ARCHITECTURE.md](../ARCHITECTURE.md) for the v1 system this plan builds
on top of.

Diagram sources: [`labukdb-v2-module-architecture.puml`](labukdb-v2-module-architecture.puml)
(PlantUML source — edit this), rendered as
[`labukdb-v2-module-architecture.pdf`](labukdb-v2-module-architecture.pdf) and
[`.png`](labukdb-v2-module-architecture.png).

![labukDB v2 module architecture](labukdb-v2-module-architecture.png)

Legend: **lavender** boxes are reused from v1 as-is (package renamed
`com.labuk.deltaflight` → `com.labuk.labukdb`); **amber** boxes are new —
they don't exist yet.

---

## Why this shape

Three decisions drive the whole layout:

1. **Don't invent a new storage/commit format — stay protocol-compatible
   with Delta.** labukDB becomes a *writer* of the same `_delta_log`
   protocol it already reads, instead of inventing its own WAL/commit
   format. A table labukDB writes stays readable by Spark, DuckDB,
   `delta-rs`, Databricks; a Spark-written table stays readable by
   labukDB. This is the single highest-leverage decision in the whole
   plan — it turns "build a database" into "build a writer for a log
   format already deeply understood as a reader."
2. **The gateway/Iceberg plan is reused whole, not replaced.** `planner/`,
   `decode/`, `storage/` don't change. `DeltaTableFormat` and
   `IcebergTableFormat` both implement the same `TableFormat` interface
   designed for the multi-format gateway — labukDB is a strict superset
   of that plan, not a different path.
3. **The embeddable core stays dependency-light.** `labukdb-core` has no
   Flight/gRPC dependency, so an application that only wants in-process
   query access (the DuckDB use case) isn't forced to pull in Netty/gRPC
   just because the project also offers a server mode.

## Two access modes, one engine

| Mode | Shape | Analogous to |
|---|---|---|
| **Embedded library** | Add `labukdb-core` (+ format modules) as a Maven dependency, call the Java API in-process, no server to run | DuckDB — the engine lives inside your JVM process |
| **Flight server** | Run `labukdb-server-app`, connect from any language with an Arrow Flight binding | Postgres/MySQL — a separate process, network protocol |

DuckDB itself only offers the first mode. labukDB offers both from the
same core, which is the actual differentiator, not an imitation of either
system alone.

## Modules

| Module | Contains (today's package) | Depends on | Produces |
|---|---|---|---|
| `labukdb-core` | `log/`, `planner/`, `decode/`, `storage/`, `catalog/` (reused) + `encode/`, `execution/`, `format/` (new) | — | embeddable library jar |
| `labukdb-format-iceberg` | ✅ **Done** — `IcebergTableFormat`, delegating manifest/table loading to `iceberg-core:1.12.0`'s `HadoopTables` | `labukdb-core`, `iceberg-core` | embeddable library jar (optional) |
| `labukdb-flight` | `flight/` (adds `DoPut`) | `labukdb-core` [+ `labukdb-format-iceberg`] | library jar |
| `labukdb-server-app` | `DeltaFlightServerApp` → `LabukDbServerApp` | `labukdb-core`, `labukdb-flight` | shaded standalone server jar |
| `labukdb-cli` | `cli/` | `labukdb-core` in-process — no Flight needed, same as today | shaded CLI executable |

## What's new inside `labukdb-core`

Everything else in the diagram (`log/`, `planner/`, `decode/`, `storage/`,
`catalog/`) is reused unchanged. Five subsystems are genuinely new:

- **`format/WriteBuffer` (Ingester)** — see [Write path: it's an LSM
  tree](#write-path-its-an-lsm-tree-at-file-granularity) below. Absorbs
  many small incoming writes in memory before they become a file.
- **`format/CommitWriter`** — the highest-risk piece. Reads the latest
  version, builds a new commit (add/remove actions), writes it atomically,
  detects conflicting concurrent commits, retries. This is where real
  database-correctness risk concentrates, not in the query path. See
  [Concurrent writes](#concurrent-writes) below.
- **`encode/ArrowToParquetWriter`** — the write-side mirror of
  `ParquetToArrowReader`; turns a flushed buffer into a Parquet file that
  `CommitWriter` then references in a commit.
- **`log/Compactor` and `log/Vacuum`** — ✅ **Done** (placed in `log/`
  alongside `CommitWriter`, not `format/` — same reasoning that already
  kept `CommitWriter` out of the `TableFormat`-SPI package). `Compactor`
  bin-packs all active files for a table+partition into one
  `OPTIMIZE`-style commit (every action `dataChange=false`, matching real
  Delta semantics — what lets incremental readers skip compaction commits
  entirely); no size/age selection heuristic yet, a named future tuning,
  not a correctness gap. `Vacuum` scans full commit history for stale
  `remove` actions past a retention window (7 days, matching real Delta's
  own default) and deletes their bytes — proven to never delete a file
  still in the *current* active set even when a stale historical commit
  claims to have removed that same path (the one safety invariant that
  actually matters). **Both also work against remote (S3/MinIO/ADLS)
  tables now** — additive `TableLocation`-based overloads, same pattern
  as `CommitWriter`; `Vacuum`'s remote delete uses Hadoop `FileSystem.delete`,
  live-MinIO-verified with a real `FileSystem.exists()` check proving
  bytes were actually gone, not just logically removed. No new concurrency
  risk from either: old bytes stay on disk until `Vacuum` runs, so an
  in-flight reader's isolation already
  holds via `Snapshot`'s existing defensive copy.
- **`log/TableMerger`** — ✅ **Done**: upsert/MERGE by key — `CommitWriter`
  alone can only blind-append or compact, never apply an update/delete
  by key. Originally motivated by a CDC source (e.g. Debezium) as the
  calling example — that specific direction was later considered and
  deliberately dropped (see the build-order section below for why); the
  upsert/MERGE capability itself stands on its own regardless of what
  calls it. One table + partition per call: `inserts`/`updates`/`deletes`
  as three separate `VectorSchemaRoot`s (not one batch with an
  operation-indicator column), applied in that order against whatever's
  currently in the partition, committed as a single atomic
  `CommitWriter.replaceFiles(..., dataChange=true)` — `true` here, not
  `Compactor`'s `false`, since row values genuinely change. **The key is
  supplied fresh on every call, never stored as table metadata** — this
  deliberately matches real Delta `MERGE INTO`'s own behavior (its `ON`
  clause is always specified at query time, confirmed via Databricks'
  docs), not Iceberg's `identifier-field-ids` (which IS persistent schema
  metadata — but that's Iceberg's protocol, and `CommitWriter`/`TableMerger`
  write Delta; storing a private `keyColumns` field in Delta's `metaData`
  action would be a non-standard extension a real Spark/delta-rs/DuckDB
  client wouldn't understand). Keys are scoped to their partition — no
  cross-partition search; a key whose partition value changes over its
  lifetime needs an explicit delete-then-insert orchestrated by the
  caller. Guards against a correctness gap `Compactor` doesn't have to
  worry about: before committing, re-checks that the partition's active
  file set is *exactly* what was read at the start of the call, not just
  "nothing I'm removing has vanished" (`replaceFiles`'s own check) —
  catching a concurrent writer having *added* a file to the same
  partition, which could otherwise collide at the key level. Verified
  with a real two-thread race (a `merge()` call and a plain concurrent
  `appendBatch` into the same partition, both landing with no data lost)
  and a real-table smoke test (insert → update → delete against a copy
  of `logs_delta`). Residual, named limitations: a narrow window remains
  between that freshness check and the actual atomic write (closing it
  needs a server-side "expected file set" precondition, out of scope);
  every retry re-reads/re-decodes the whole partition, same cost
  `Compactor` already accepts; no schema-type validation beyond "does
  this named column exist."
- **`execution/`** — ✅ **Done**: `AggregateOperator` (`GROUP BY` +
  `SUM`/`COUNT`/`MIN`/`MAX`/`AVG`) and `SortLimitOperator` (`ORDER BY`/
  `LIMIT`, built on `arrow-algorithm`'s `IndexSorter`/`DefaultVectorComparators`
  rather than hand-rolled comparison logic), proven individually and
  composed together, tested including a 50-row case that genuinely
  exercises `IndexSorter`'s quicksort path, not just small-array
  insertion sort. `FilterOperator` deliberately **not** built — the
  existing fused residual-predicate pass in `decode/GroupPredicateEvaluator`
  already filters rows during decode; a separate post-decode operator
  would have no real caller yet (same premature-abstraction reasoning
  that deferred `TableFormat`).
- **`execution/` wired into `flight`** — ✅ **Done**: the real protocol
  decision named above — `GetFlightInfo`'s one-endpoint-per-file model
  genuinely didn't fit `GROUP BY`'s need to combine rows across files —
  is resolved by switching to a **single combined endpoint** whenever a
  query's `groupBy`/`aggregates`/`orderBy`/`limit` is present (any one of
  the four, not just `GROUP BY` — a plain global top-N needs the exact
  same cross-file combination a `GROUP BY` does). `DoGet` for that
  endpoint decodes every surviving file, combines them (same
  `VectorSchemaRootAppender` pattern `Compactor`/`TableMerger` already
  use), then runs `AggregateOperator`/`SortLimitOperator` server-side
  before streaming back one result. A plain scan (none of those four
  fields) stays on the original, completely unchanged one-file-per-endpoint
  path — verified by the existing `FlightIntegrationTest` suite passing
  unmodified. New `FlightTicket` sealed interface
  (`ScanTicket`/`CombinedScanTicket`) is what `TicketRegistry`/`DoGet`
  dispatch on. Verified with a real server+client test spanning multiple
  physical files for both the `GROUP BY` case and the plain
  `ORDER BY ... LIMIT` case, each asserting the *globally* correct
  result, not a per-file-local one. `COUNT` remains `COUNT(column)`
  (non-null values), not `COUNT(*)` — unchanged, pre-existing
  `AggregateOperator` semantics, not something this pass altered.
  Deliberately not done here: a SQL parser (sequenced after this — SQL
  needs an execution path to target, which is what this pass built) and
  distributed cross-node aggregation (single-process only, same as
  everything else in this codebase).

`flight/DoPut` is the ingest entry point: it hands incoming Arrow batches
to `WriteBuffer` first, not straight to `ArrowToParquetWriter`.

## Write path: it's an LSM tree, at file granularity

`CommitWriter` + `Compactor` are already structurally an LSM tree — just
applied to whole files instead of individual rows, which is exactly the
shape Delta/Iceberg/Hudi and IOx (underneath InfluxDB 3.0) all share:

| Classic LSM (RocksDB/Cassandra) | labukDB (file-level LSM) |
|---|---|
| Memtable (in-memory sorted writes) | `WriteBuffer` |
| WAL (durability before flush) | `WriteBuffer`'s WAL |
| SSTable (immutable sorted file) | a committed Parquet file |
| Compaction (merge small SSTables) | `Compactor` |
| Tombstone / merge-on-read | `remove` actions in the commit log |

**Why `WriteBuffer` specifically earns its place, not just completeness
for the table above:** without it, every `DoPut` call would flush
straight to one new Parquet file + one new commit. Under frequent,
small writes — the exact ingestion pattern a time-series/metrics workload
produces — that's a tiny file and a log entry per call, and `Compactor`
would be permanently fighting a losing battle instead of doing periodic
cleanup. `WriteBuffer` buffers per table in memory, backed by a WAL for
crash durability, and only flushes to `ArrowToParquetWriter` →
`CommitWriter` when a size or time threshold is hit (e.g. 128 MB or 60
seconds, whichever comes first). It also serves reads directly — a query
sees its own very recent, not-yet-flushed writes by checking the buffer
alongside committed files, the classic LSM "check the memtable, then the
SSTables" read path. (This is also, concretely, what the "Arrow columns
in memory to cache hot data" box in InfluxDB 3.0's own architecture
diagram is — not just a read cache, the write buffer itself.)

## Concurrent writes

Since labukDB stays Delta-protocol-compatible, it inherits Delta's own
answer to concurrency rather than inventing one — optimistic concurrency
control (OCC):

1. Writer reads the latest version `V` (`SnapshotManager.latest()`).
2. Builds its change set (add/remove actions) against that snapshot.
3. Attempts to commit as `V+1` via an **atomic create-if-absent** write of
   `_delta_log/<V+1>.json` — must fail, not silently overwrite, if the
   file already exists.
4. Success → done. Failure (someone else won the race for `V+1`) →
   re-read the new latest version, check whether the winning commit's
   actions actually conflict with this writer's read/write set. No real
   conflict → rebase and retry as `V+2`. Real conflict → fail the
   transaction and surface it to the caller.
5. Bounded retries with backoff, to avoid livelock under contention.

The algorithm (steps 1–5) is well-documented and mechanical — **the
actual risk is step 3's atomic create-if-absent, per storage backend**,
and it's not uniform:

| Backend | Atomic create-if-absent | Status |
|---|---|---|
| Local disk | `Files.createFile(path, CREATE_NEW)` | Done, trivially atomic |
| S3 (native AWS) | `If-None-Match: *` conditional PUT, via Hadoop ≥3.4.2's S3A support ([HADOOP-19256](https://issues.apache.org/jira/browse/HADOOP-19256)) | **Implemented** (`log/HadoopLogStorage`, Hadoop bumped to 3.4.3) — verified only via local-filesystem substitution (`HadoopLogStorageTest`), **not a real AWS endpoint** |
| MinIO | Same S3A code path | **Implemented and verified against a real cluster** (`quay.io/minio/minio:latest`) — create, read-back, conflicting second create correctly threw `FileAlreadyExistsException`, original content survived. Not a guarantee for every MinIO build, but this common image works. |
| ADLS Gen2 | Create-without-overwrite-first, via `abfs` ([HADOOP-17215](https://issues.apache.org/jira/browse/HADOOP-17215), already in Hadoop 3.3.0+) | **Implemented**, same code path as S3/MinIO — **unverified against a real ADLS endpoint** |

Real Delta Lake OSS historically needed an external coordinator (a
DynamoDB table) for S3 specifically, before S3 gained native conditional
writes. This project would have hit the same problem if it had assumed
otherwise without checking — which is exactly why this was researched
before being implemented, not assumed.

**Update — multi-writer OCC is now done, for local disk.** `CommitWriter.appendBatch`
and `.replaceFiles` both retry under optimistic concurrency control when
the atomic commit-file create loses a race, bounded (10 attempts) with
randomized backoff. `appendBatch` retries unconditionally (a blind append
never conflicts with another, at the data level — only who gets which
version number). `replaceFiles` re-checks on every attempt that its
`filesToRemove` are still in the active set, and throws
`ConcurrentModificationException` rather than retrying blindly if another
writer already removed one of them. Verified with a real 10-thread
concurrent-`appendBatch` stress test (no lost updates, no duplicate
version numbers, across 3 repeated runs) and a real-table smoke test (5
genuinely concurrent writers against a copy of `logs_delta`, all 5
landed correctly). Remote (S3/MinIO/ADLS) OCC conflict-retry logic is
no longer blocked by `CommitWriter` being local-only (see the next
update) — `commitWithRetry` now catches both the NIO and Hadoop
`FileAlreadyExistsException` classes, which really are unrelated classes
despite the shared simple name — but it hasn't been separately
stress-tested under real concurrent remote writers the way the local
path has (the 10-thread test above is local-disk only).

**Update — CREATE TABLE and remote write completion are now done.**
`CommitWriter`, `ArrowToParquetWriter`, and `WriteBuffer` are all
`TableLocation`-dispatched now (local or remote, same pattern
`SnapshotManager`/`ParquetToArrowReader` already used for reads) via
additive `forLocation`/`TableLocation`-based constructors — the original
local-only constructors are unchanged, zero blast radius on
`Compactor`/`TableMerger`, which stayed local-only by design *at the
time* (both later gained the same additive remote support — see the
build-order section below). `DoPut` no longer rejects remote table
locations. A new `TableCreator` class writes
a table's version-0 commit (`protocol` + `metaData`, no `add` actions) —
the capability this project never had before; every fixture until now
hand-wrote an initial commit directly. `ArrowToDeltaSchema` is the new
Arrow `Schema` → Delta `schemaString` encoder `TableCreator` needs,
Jackson-tree-based rather than the hand-concatenated strings every test
fixture used as a shortcut.

**`WriteBuffer`'s WAL deliberately stays local-disk-only** even for a
remote table — it's server-internal durability staging, never part of
the Delta protocol or visible to any other reader, so redirecting it to
S3/MinIO would trade away the latency/cost reason it exists for zero
extra correctness verification. Its new `TableLocation`-based
constructor takes an explicit `walRoot` parameter (defaulting to
`~/.labukdb/wal`, same convention `TableCatalog.atDefaultLocation()`
uses for `~/.labukdb/catalog.json`) rather than deriving one from the
table's own — now possibly remote — location.

Verified live against real MinIO, both at the library level
(`RemoteWriteMinioIntegrationTest`: create a table, append real data,
read it back) and through the actual user-facing Flight path
(`FlightRemoteWriteMinioIntegrationTest`: a real `DoPut` against a live
MinIO-backed table, then a real `DoGet` reading it back) — the literal
"Flight serving — write: 0% remote" gap this closes.

**Deliberately not done here:** a Flight `DoAction` RPC or CLI
subcommand for CREATE TABLE — `TableCreator` is a library capability
only for now, each of those needs its own protocol/UX decision. Iceberg
write support, and remote multi-writer OCC stress-testing (see above).

## Build order

Not all five new subsystems carry equal risk — ordered to get something
demoable early and defer the highest-risk, least-verified piece until the
foundation under it is proven:

1. ✅ **Single-writer commit path** — `CommitWriter` + `ArrowToParquetWriter`. Done.
2. ✅ **`WriteBuffer`** — the memtable/WAL layer. Done, including real crash recovery.
3. ✅ **`DoPut` on Flight** — wires the ingest entry point to (1) + (2). Done,
   verified against a real server with a real `pyarrow.flight` client.
4. ✅ **Compaction / vacuum.** Done — `Compactor` + `Vacuum`, real-table verified.
5. ✅ **`execution/`** — `AggregateOperator` + `SortLimitOperator` done,
   standalone and composed, and now wired into `flight` too (see above).
6. ✅ **Multi-writer OCC (local disk)** — done, see above. Remote OCC
   (S3/MinIO/ADLS) remains a separate, later piece — `CommitWriter` is
   still local-only.
7. ✅ **`TableMerger` — upsert/MERGE by key.** Done, see above.
   **A Debezium CDC connector was considered and deliberately dropped** —
   Debezium only makes sense as a bridge from an *external* OLTP database
   (Postgres/MySQL/etc.) that some other application already writes to,
   not something labukDB itself needs; it doesn't fit labukDB's actual
   target (a standalone real-time OLAP database applications write to
   directly) any more than it fits DuckDB's (confirmed DuckDB has no
   CDC-ingestion equivalent either — its Postgres/MySQL extensions are
   query-time federation, not replication). `TableMerger` remains useful
   on its own regardless — upsert-by-key doesn't need a CDC source.
8. ✅ **CREATE TABLE + remote write completion.** Done, see above —
   `TableCreator`, and `CommitWriter`/`ArrowToParquetWriter`/`WriteBuffer`
   all `TableLocation`-dispatched, live-MinIO-verified at both the
   library and Flight levels.
9. ✅ **`execution/` wired into `flight`.** Done, see above —
   `GROUP BY`/`ORDER BY`/`LIMIT` now reachable through a real `DoGet`
   round trip, combined correctly across multiple files, **live-MinIO-verified
   for remote too** (`FlightRemoteAggregationMinioIntegrationTest`). A SQL
   layer (deliberately deferred until this existed) is the natural next step.
10. ✅ **Remote completion for the remaining local-only pieces —
    `TableMerger`, `Compactor`, `Vacuum`, and Iceberg reads.** All four
    were either genuinely local-only (the first three — hardcoded `Path`/
    local `SnapshotManager`/`CommitWriter` construction throughout) or
    structurally remote-capable but never actually run against a live
    cluster (`IcebergTableFormat` — its constructor already took an
    arbitrary location string + `Configuration`). `TableMerger`/`Compactor`
    each gained an additive `TableLocation`-based overload, same pattern
    as `CommitWriter`/`ArrowToParquetWriter`/`WriteBuffer` before them —
    old `Path`-based public methods completely unchanged, zero blast
    radius on existing callers/tests. `Vacuum` needed one genuinely new
    piece on top of that: a `TableLocation`-aware file **delete** (local →
    `Files.deleteIfExists`; remote → Hadoop `FileSystem.delete`), since
    nothing in this codebase had needed to delete a data file remotely
    before. All four live-MinIO-verified, including the one assertion
    that actually matters for `Vacuum`: a real `FileSystem.exists()`
    check against the bucket proving bytes were genuinely deleted, not
    just that a log entry claims so.

**Known, pre-existing flaky test — not caused by the work above.**
`labukdb-flight`'s `FlightIntegrationTest` intermittently fails (~1-in-4
observed) with `IllegalStateException: Memory was leaked by query` during
`@AfterEach`'s `allocator.close()`. Confirmed not specific to any one
test — it's hit a pure-read test (`partitionPruningAndResidualFilter...`,
untouched by this session's write-path changes) as often as a `DoPut`
one, and Arrow's own issue tracker has the identical error message on
Arrow's own Flight test suite, explicitly concluded to be a flake, not a
real bug ([ARROW-4448](https://issues.apache.org/jira/browse/ARROW-4448)).
Left alone deliberately rather than spending effort hardening something
upstream also treats as environmental gRPC/Flight teardown timing, not a
logic defect — revisit if it starts failing CI at a materially higher
rate than observed here.

## Phase roadmap beyond v1 core

**Correction to an earlier framing:** a previous pass at this plan called
distributed ingestion and distributed query execution "explicitly out of
scope." That was wrong — they're required for the full architecture and
stay on the roadmap. What's true is only that they come *after* v1 core
(the write path above) is built and validated, not that they're dropped.
v1 core is the current priority; these are sequenced, not shelved.

| Phase | Scope | Status |
|---|---|---|
| `CommitWriter` (single-writer) + `encode/` | Build-order steps 1 | ✅ Done — tested against a fixture and a real table |
| `WriteBuffer` | Build-order step 2 | ✅ Done — auto-flush, crash recovery, tested |
| Remote atomic commits (S3/MinIO/ADLS) | `HadoopLogStorage.createCommitFile`, Hadoop bumped to 3.4.3 | ✅ Implemented; **MinIO real-infra verified**, S3/ADLS implemented-but-unverified |
| `format/TableFormat` + `DeltaTableFormat` | Additive SPI prep | ✅ Done, tested standalone |
| **Multi-format — `labukdb-format-iceberg`** | `IcebergTableFormat`, using `iceberg-core:1.12.0` (not hand-rolled) | ✅ **Done** — read-only, path-based (`HadoopTables`, no catalog service). Validated the `TableFormat` shape as intended: surfaced two real mismatches (`atVersion`'s Delta-sequential assumption vs. Iceberg's non-sequential `snapshotId`; `schemaString`'s Delta-JSON shape vs. Iceberg's typed `Schema`) — named, not silently papered over. Also empirically proved `decode/ParquetToArrowReader` is format-agnostic: reads an Iceberg-committed file with zero changes. Tested against a self-built fixture table (`iceberg-core`'s own create API), not a Spark/Trino-written one — same caveat `CheckpointReader` carries for Delta checkpoints. **Remote (MinIO) now live-verified too** — the constructor was always location-agnostic (`HadoopTables` resolves via Hadoop's own `FileSystem`), just never actually run against a live cluster before; Iceberg's own file paths are absolute, not relative like Delta's, confirmed from how the fixture-building code registers them. |
| `flight/DoPut` | Ingest endpoint | ✅ Done — local and remote, real `pyarrow.flight` client (local) and real `FlightClient` against live MinIO (remote) both verified |
| `TableFormat` wired into `planner`/`flight`/`cli` | Still deferred | Two implementations now exist and have already informed the interface (see mismatches above) — wiring itself still not done |
| Read scale-out | Self-describing Flight tickets (encode the scan directly in the ticket bytes instead of a `TicketRegistry` lookup), stateless server replicas behind a load balancer | Not started — cheap once started, no new distributed-systems machinery needed since snapshot resolution is already stateless against shared object storage |
| CREATE TABLE | `TableCreator`, local + remote | ✅ **Done** — library capability only, not exposed over Flight/CLI yet |
| Multi-writer OCC — local disk | Retry + conflict detection in `CommitWriter` | ✅ **Done** — real 10-thread + real-table concurrency tests, see "Concurrent writes" above |
| Multi-writer OCC — remote (S3/MinIO/ADLS) | `commitWithRetry` now catches both NIO and Hadoop `FileAlreadyExistsException` classes | Conflict-retry logic in place; not separately stress-tested under concurrent remote writers the way local disk is |
| **Distributed ingestion fleet** | Router + sharded Ingesters + a durable distributed WAL (not local-disk-per-node) + a catalog service tracking which node owns which table's live buffer — IOx's Router/Ingester split | **Planned, not yet designed.** Genuinely the largest remaining piece — closer to a distributed-systems project than an extension of the single-node write path, and needs its own dedicated design pass once v1 core is real |
| **Distributed query execution** | Splitting one query's work across multiple compute nodes (join/aggregate pushdown across a fleet, not just client-side fan-out over `FlightEndpoint`s) | **Planned, not yet designed.** Needs a real distributed query planner; revisit once `execution/` exists single-node and its limits are actually felt |

---

*This document and its diagrams describe a plan, not current behavior.
Update both the `.puml` source and this file together as the design
changes — don't let them drift the way `ARCHITECTURE.md` briefly did
about nested-type support.*
