# labukDB v2 — Multi-Module Architecture

**Status: mostly built.** This document captures the design for turning
`delta-flight-engine` (a read-only Delta Lake gateway) into **labukDB** —
a multi-format lakehouse database that reads and writes Delta tables,
reads Iceberg tables, and is usable either as an embedded Java library or
as a Flight server. Most of what's described here is now implemented and
verified, local and remote (live-MinIO) — the "Build order" and "Status"
sections below track exactly what's done vs. still planned, item by
item; don't assume this header alone reflects the current state of any
one piece. See [ARCHITECTURE.md](../ARCHITECTURE.md) for the v1 system
this plan builds on top of, and [USAGE.md](../USAGE.md) for how to
actually use what's built.

Diagram sources: [`labukdb-v2-module-architecture.puml`](labukdb-v2-module-architecture.puml)
(PlantUML source — edit this), rendered as
[`labukdb-v2-module-architecture.pdf`](labukdb-v2-module-architecture.pdf) and
[`.png`](labukdb-v2-module-architecture.png).

![labukDB v2 module architecture](labukdb-v2-module-architecture.png)

Legend: **lavender** boxes are reused from v1 as-is (package renamed
`com.labuk.deltaflight` → `com.labuk.labukdb`); **green** boxes are new
in v2 and built + verified (most of the diagram, at this point); **amber**
boxes are new and still only planned — they don't exist yet.

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
2. **The gateway/Iceberg plan is reused whole, not replaced.** `decode/`,
   `storage/` don't change. `DeltaTableFormat` and `IcebergTableFormat`
   both implement the same `TableFormat` interface designed for the
   multi-format gateway — labukDB is a strict superset of that plan, not
   a different path. (Update: `planner/` later gained an additive
   `TableFormat`-based counterpart — `QueryPlanner.planNormalized`/
   `NormalizedScanPlan` — once Iceberg's CLI/Flight query path needed it;
   Delta's own `plan`/`ScanPlan` are unchanged, see the Iceberg wiring
   update below.)
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
| `labukdb-core` | `log/`, `planner/`, `decode/`, `storage/`, `catalog/` (reused) + `encode/`, `execution/`, `format/`, `query/` (new) | — | embeddable library jar |
| `labukdb-format-iceberg` | ✅ **Done** — `IcebergTableFormat` (reads) + `IcebergTableWriter` (append-only writes), path-based (`HadoopTables`) or catalog-based (`Catalog`/`TableIdentifier`, incl. `IcebergRestConfiguration`/`IcebergRestLocation` for `icebergrest://` REST-catalog references) | `labukdb-core`, `iceberg-core` | embeddable library jar (optional for a pure core embedder; required by cli/flight) |
| `labukdb-flight` | `flight/` (adds `DoPut`, including Iceberg append) | `labukdb-core`, `labukdb-format-iceberg` (Iceberg query + write support) | library jar |
| `labukdb-server-app` | `DeltaFlightServerApp` → `LabukDbServerApp` | `labukdb-core`, `labukdb-flight` | shaded standalone server jar |
| `labukdb-cli` | `cli/` | `labukdb-core`, `labukdb-format-iceberg` (for Iceberg `--table`/`--sql` support) in-process — no Flight needed | shaded CLI executable |

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
  Deliberately not done here: distributed cross-node aggregation
  (single-process only, same as everything else in this codebase). A SQL
  parser was sequenced after this (SQL needs an execution path to
  target) and is now done — see the SQL query layer update below.

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
| MinIO | Same S3A code path | ⚠️ **Confirmed NOT atomic under real concurrency, on at least one real deployment.** A single *sequential* conflict (create, then a deliberately conflicting second create) does throw `FileAlreadyExistsException` correctly (`HadoopLogStorageMinioIntegrationTest`) — but that is not the same claim as "safe for concurrent writers," and it isn't one. An isolated diagnostic (8 threads calling `fs.create(path, overwrite=false)` on the *identical* key, simultaneously, no `CommitWriter` logic involved) found all 8 "succeeded" and zero conflicts were detected — see `CommitWriterConcurrentMinioIntegrationTest`, which fails on purpose as a standing proof of this gap. Matches a documented Hadoop caveat: [HADOOP-19256](https://issues.apache.org/jira/browse/HADOOP-19256)'s own release notes warn conditional-write support "may not be supported on third party stores" and "can't be confirmed without testing" — this is exactly that case, confirmed. |
| ADLS Gen2 | Create-without-overwrite-first, via `abfs` ([HADOOP-17215](https://issues.apache.org/jira/browse/HADOOP-17215), already in Hadoop 3.3.0+) | **Implemented**, same code path as S3/MinIO — **unverified against a real ADLS endpoint**, and given the MinIO finding above, not safe to assume correct without the same kind of concurrent-writer test |

Real Delta Lake OSS historically needed an external coordinator (a
DynamoDB table) for S3 specifically, before S3 gained native conditional
writes. **This project just rediscovered exactly why, the hard way, on a
real third-party S3-compatible store.** Relying on
`fs.s3a.create.conditional.enabled` alone is known to be insufficient for
this project's actual multi-writer correctness requirement on at least one
real deployment.

**Update — the fix is now done: an opt-in `LockProvider` (`log/LockProvider.java`,
one implementation `log/DynamoDbLockProvider.java`), engaged via
`CommitWriter.forLocation(location, lockProvider)`.** `CommitWriter.forLocation(location)`
(no lock) is unchanged and remains the default. Full write-path crash
recovery, mirroring real Delta's own `io.delta.storage.BaseExternalLogStore`
algorithm precisely (confirmed from its real source, not invented): a
commit writes its content to a temp file first, claims an incomplete
external lock entry (the real mutual-exclusion point), writes the final
file, then marks the entry complete; the next writer recovers any stuck
entry for the version it's about to attempt before proceeding — a design
bug found and fixed during implementation, not assumed correct the first
time (checking only version N-1 for a stuck entry doesn't work, since the
stuck slot is N itself; see `CommitWriter.catchUpViaLock`'s javadoc for the
full reasoning).

**Scope, stated precisely — a mitigation for non-compliant S3-compatible
object storage, not a universal multi-cloud requirement.** Real AWS S3
(conditional writes since late 2024), real Google Cloud Storage
(strongly-consistent generation-precondition creates for years, per
Google's own docs), and real Azure Blob Storage/ADLS (`If-None-Match: *`, a
long-standing native REST feature) are all documented as reliable without
any external lock — the gap this fixes is specifically third-party
S3-compatible re-implementations (this real MinIO deployment being the
confirmed case), matching HADOOP-19256's own caveat about third-party
stores, not S3 itself. (Documented, not independently re-verified the way
the MinIO failure was — same honesty standard applied throughout.) A real
AWS/Azure/GCP deployment should likely never need to configure a
`LockProvider` at all; it exists for self-hosted/non-compliant storage.
`DynamoDbLockProvider` was chosen as the one shipped implementation because
it works identically against real AWS DynamoDB or a self-hostable
`amazon/dynamodb-local` container (zero new Maven dependency — already
transitively present via `hadoop-aws`'s bundled AWS SDK), so a MinIO-only
deployment doesn't need an AWS account to get a real lock.

**Verified empirically, matching the rigor that found the original bug:**
a live 8-thread conditional-`PutItem` race against `amazon/dynamodb-local`
produced exactly 1 success and 7 clean `ConditionalCheckFailedException`s —
the real atomicity MinIO's own conditional-create lacks. The actual
end-to-end proof — the same 8-concurrent-writer-against-real-MinIO scenario
that fails without the lock — passes cleanly with it engaged
(`CommitWriterConcurrentMinioLockedIntegrationTest`). A deterministic
local-only test (`CommitWriterLockRecoveryTest`) exercises the crash-recovery
branch directly, since the live-infra tests structurally can't trigger it
(8 simultaneous non-crashing writers never leave a stuck entry behind). The
original unlocked test (`CommitWriterConcurrentMinioIntegrationTest`) is
left untouched, still failing on purpose, as the standing proof of the
underlying storage-layer gap this feature mitigates.

**Known, documented limitation, not solved:** write-path-only recovery (the
next writer repairs a stuck prior entry before proceeding) — not read-path
proactive recovery. A table abandoned mid-write and never written to again
before the lock entry's TTL expires stays unrecovered; real Delta's own
design carries the identical limitation. Also: every writer of a given
table must agree on using the same `LockProvider` (same backing table), or
none at all — one writer bypassing it defeats the scheme for that table.

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

**Update — SQL query layer is now done, on the CLI.** New `core/query/`
package: `SqlQueryTranslator` parses a real `SELECT` statement (via
`jsqlparser`, not a hand-rolled grammar — parser theory isn't one of
this project's own from-scratch learning goals, unlike Delta/Parquet/
pruning/execution) straight into the existing `Predicate`/`AggregateSpec`/
`SortKey` structures `execution/` already executes correctly, returned
as a new `SqlQuery` record. Two real bugs were found and fixed by
actually running the translator against `jsqlparser`'s real parse output
(via `javap`/a throwaway diagnostic program, not assumed from memory):
`COUNT(*)` does not set `Function.isAllColumns()` in this grammar path —
it shows up as a single parameter of type `AllColumns` instead, which is
what's actually checked now; and a bare `col IN (...) OR ...` lets `IN`'s
grammar production greedily consume the trailing `OR`/`AND` as its own
right-hand expression instead of binding tighter than `OR` as standard
SQL precedence would, which is rejected with a specific error naming the
fix (wrap the `IN` predicate in parentheses) rather than silently
misinterpreted.

**A real, related gap surfaced while scoping this, not invented:** the
CLI's own `QueryCommand` never had `execution/` wired in either — only
scan+filter+project, with `--limit` just capping total printed rows, not
a real `ORDER BY`/`LIMIT`. Since `labukdb-cli` depends on `labukdb-core`
only (deliberately no Flight dependency), SQL on the CLI couldn't deliver
`GROUP BY`/`ORDER BY` without `execution/`'s combine logic existing
somewhere both `labukdb-cli` and `labukdb-flight` can reach — so it was
extracted into `labukdb-core` itself: new `execution/CombinedQueryExecutor`
+ `execution/FileScan` (moved out of being a `CombinedScanTicket`-local
record in `labukdb-flight`), holding the exact decode → combine →
aggregate → sort logic verbatim from `LabukDbFlightProducer.getStream`'s
`CombinedScanTicket` branch. `LabukDbFlightProducer` now calls this shared
executor instead of its own inline copy — verified behavior-preserving by
re-running `FlightAggregationIntegrationTest`/`FlightIntegrationTest`
unmodified after the extraction.

CLI: `--sql "<SELECT ...>"`, mutually exclusive with
`--table`/`--columns`/`--filter`/`--limit` (the SQL text subsumes all
four). A plain `SELECT`+`WHERE` (no `GROUP BY`/aggregates/`ORDER BY`/
`LIMIT`) reuses the existing per-file streaming scan unchanged — only a
query needing cross-file combination (same `combineMode` trigger
`LabukDbFlightProducer` uses) goes through `CombinedQueryExecutor`. Unlike
`--limit` (defaults to capping output at 50 rows), a bare SQL `SELECT`
with no `LIMIT` clause prints every matching row, matching real SQL
semantics rather than silently truncating. Verified with a new
`QueryCommandSqlIntegrationTest` against a real two-partition-file
fixture — genuine cross-file `GROUP BY`+`COUNT` and a genuine global
top-N `ORDER BY ... LIMIT`, the same correctness bar
`FlightAggregationIntegrationTest` already holds Flight to.

**Deliberately not done here:** SQL-over-Flight (translating SQL text
into the `FlightQueryRequest` JSON shape server-side) — a client still
builds that JSON shape directly; this pass is CLI-only.

**Update — `--sql` is now live-verified against a real MinIO-backed
table too**, closing a real coverage gap: the MinIO-verified aggregation
path was previously only exercised via Flight's `combineMode`, and the
CLI's own SQL entry point had never actually been driven against a remote
table directly — correctness was inferred from shared plumbing, not
confirmed. `QueryCommandSqlMinioIntegrationTest` issues real `--sql`
commands (`GROUP BY`+`COUNT`, `ORDER BY ... LIMIT`, `WHERE`) against a
table built with real `TableCreator`/`CommitWriter` calls against live
MinIO — all three pass.

**Update — Iceberg reads are now wired into `cli`/`flight`, closing a
real honesty gap.** Confirmed via grep before this change: zero
references to `IcebergTableFormat` existed anywhere in `labukdb-cli` or
`labukdb-flight` — an Iceberg table was readable only by writing Java
directly against it, not through `--table`, `--sql`, or Flight, even
read-only. New `format/DetectedFormat` enum auto-detects which format a
`TableLocation` holds by probing for `_delta_log/` (Delta) vs.
`metadata/` (Iceberg) — the one new generic primitive this needed,
`TableLocation.exists(String)`.

**A real module-dependency constraint shaped this:** `labukdb-core`
cannot reference `IcebergTableFormat` — `labukdb-format-iceberg` depends
on `labukdb-core`, not the reverse, and that can't become circular. So
detection lives in core (it only needs `TableLocation`), but constructing
the right `TableFormat` implementation had to move into `labukdb-cli`
and `labukdb-flight` themselves — both gained `labukdb-format-iceberg` as
a new, non-optional dependency (same override precedent as
`hadoop-aws`/`hadoop-azure`/`jsqlparser`: optional for a pure
`labukdb-core` embedder, required in the actual shipped binaries that
now genuinely query Iceberg tables).

**Delta's existing query path is completely untouched** — `QueryPlanner.plan`/
`ScanPlan`/`AddFile`, in both `QueryCommand` and `LabukDbFlightProducer`,
behave exactly as before (confirmed by re-running `FlightIntegrationTest`/
`FlightAggregationIntegrationTest`/`QueryCommandSqlIntegrationTest`/
`DeltaTableFormatTest`/`IcebergTableFormatTest` unmodified — all green).
`Snapshot` is built from real Delta commit-log actions in a way nothing
can fake for Iceberg, so a real parallel path was necessary, not a
refactor of the first: new `planner/NormalizedScanPlan` +
`QueryPlanner.planNormalized`, and `PartitionPruner`/`FileStatsPruner`
each gained a `pruneNormalized` overload reusing their *existing*
`mightMatch` logic unchanged (it only ever needed a partition-values
`Map<String,String>` and a nullable stats string, never `AddFile`
itself, so the new `DataFileRef`-based overloads are thin wrappers, not
reimplementations). `FileScan`/`CombinedQueryExecutor`/
`ParquetToArrowReader`'s `TableLocation` overload needed **zero** changes
— already format-agnostic, confirmed before writing anything.

**Deliberate v1 simplification:** Iceberg queries always run through
`CombinedQueryExecutor` — even a plain `SELECT` with no `GROUP BY`/
`ORDER BY`/`LIMIT` — rather than getting its own parallel
per-file-streaming implementation the way Delta's plain-scan path has.
Correctness isn't affected; large Iceberg results just don't stream
incrementally yet. On Flight this means Iceberg always registers exactly
one `CombinedScanTicket`, reusing the *existing* ticket/executor/`getStream`
dispatch completely unchanged (already polymorphic over `FlightTicket`).
`DoPut` against a detected-Iceberg location was rejected with a clear
error in this pass, before touching anything — since reading it this way
had just made Iceberg tables visible/reachable without yet having a write
path for them. **That rejection was itself replaced by a real write path
in a later pass — see the Iceberg writes update below.**

Verified with new real-fixture tests (same `HadoopTables.create` +
`ArrowToParquetWriter` + commit pattern `IcebergTableFormatTest` already
used, extended to two partition files): `QueryCommandIcebergIntegrationTest`
(CLI `--table`/`--filter` and `--sql`, including genuine cross-file
`GROUP BY`) and `FlightIcebergIntegrationTest` (real server + client,
plain scan and `GROUP BY` over real `DoGet`).

**Update — Iceberg writes (append-only) are now done, replacing the
`DoPut` rejection above.** Revised scope after checking `iceberg-core`'s
real API rather than assuming: this isn't "a second parallel write
engine" the way it first sounded. `iceberg-core`'s own `Table`/
`AppendFiles` already implements the commit protocol — manifest writing,
snapshot creation, atomic catalog-pointer swap, **and OCC retry**
(confirmed via `javap -c` on `SnapshotProducer.commit()`'s real bytecode:
it already runs `Tasks.foreach(...).retry(...).exponentialBackoff(...)
.onlyRetryOn(CommitFailedException.class)...` internally). Unlike
`CommitWriter`, which had to hand-build its own bounded-retry loop
because this project reimplements Delta's log format from scratch, the
new `labukdb-format-iceberg/IcebergTableWriter` needs no retry loop of
its own — it's a thin adapter, much like `IcebergTableFormat`'s read side
already is: `appendBatch(VectorSchemaRoot, Map<String,String>)`, same
shape as `CommitWriter.appendBatch` for discoverability, writes via the
*existing, unmodified* `ArrowToParquetWriter`/`TableLocation` dispatch,
builds a `DataFile` via `DataFiles.builder(table.spec())` (the exact
builder calls already proven correct in `IcebergTableFormatTest`/
`IcebergRemoteMinioIntegrationTest`'s own fixtures), then
`table.newAppend().appendFile(dataFile).commit()`.

Scoped deliberately narrow, mirroring how Delta's own write path was
built incrementally (single-writer commit path first, `TableCreator`
much later): append only, to an **already-existing** table — no CREATE
TABLE, merge/upsert, compaction, or vacuum equivalent. Reachable via
Flight `DoPut` (new `acceptPutIceberg` helper, mirroring how
`icebergFlightInfo` was already factored out of `getFlightInfo`) and the
embedded library — deliberately **not** a CLI subcommand, matching
Delta's own CLI, which has no write subcommands for either format. No
Iceberg `WriteBuffer` equivalent either — one `DoPut` call is exactly one
Iceberg commit (streamed batches combined via the same
`VectorSchemaRootAppender` pattern `CombinedQueryExecutor`/`Compactor`/
`TableMerger` already use), no cross-call buffering; `WriteBuffer` was
itself a later addition to Delta's write path for the same reason, so
the same build-order logic applies here.

One real edge case resolved by a dedicated test, not assumed: an
unpartitioned table's partition path is `""`, and `DataFiles.Builder
.withPartitionPath("")` needed confirming it's accepted — it is.

Verified with `IcebergTableWriterTest` (append + read back through the
*existing, unmodified* `IcebergTableFormat`/`ParquetToArrowReader`;
append twice, both commits visible; the unpartitioned-table case) and a
real `DoPut`-then-`DoGet` round trip added to `FlightIcebergIntegrationTest`
(replacing its now-obsolete rejection test, which asserted behavior this
pass intentionally replaced).

**Update — Iceberg REST catalog integration is now done, closing the
second (and last) of the two blockers behind "Delta DB with an Iceberg
reader" vs. a genuine cross-format engine.** Until now, Iceberg tables
were only addressable path-based (`HadoopTables`) — the minority case in
real deployments, where tables are almost always registered in a catalog
service instead.

**Real API findings that shaped this, confirmed before writing anything:**
`RESTCatalog`/`HadoopCatalog`/`JdbcCatalog`/`InMemoryCatalog` are all
already bundled in `iceberg-core:1.12.0` (`unzip -l`), and
`httpclient5`/`httpcore5`/`httpcore5-h2` — the HTTP client `RESTCatalog`
needs — are already transitively resolved on `labukdb-format-iceberg`'s
classpath (`mvn dependency:tree`; `iceberg-core` declares `httpclient5`
as a plain `runtime` dependency, not optional). **Zero new Maven
dependency for REST catalog support.** `Catalog` (the interface
`RESTCatalog`/`HadoopCatalog`/`InMemoryCatalog` all share) exposes
exactly `loadTable(TableIdentifier)` — the one method a thin adapter
needs. `TableIdentifier.parse(String)` already handles dotted
multi-level namespace syntax natively. Real config keys confirmed via
`javap -constants` on `CatalogProperties`/`OAuth2Properties`/
`AuthProperties`: `uri`, `warehouse`, `rest.auth.type`, `token`,
`credential`, `oauth2-server-uri`, `rest.auth.basic.username`/`password`.

**A real footgun this design had to avoid:** `TableLocation.isRemoteUri`'s
generic `scheme://` regex matches a new `icebergrest://` scheme too, so
without an earlier, explicit interception, the existing code would hand
it to Hadoop's `FileSystem` resolution and fail with a confusing "No
FileSystem for scheme" error instead of routing it correctly. New
`format/`-adjacent `IcebergRestLocation.isRestUri(raw)` (in
`labukdb-format-iceberg`, since detecting a *catalog* reference isn't
`core`'s concern the way detecting Delta-vs-Iceberg-by-directory-probe
is) is checked *before* any `TableLocation` is constructed, in both
`QueryCommand` and `LabukDbFlightProducer`.

**Addressing, deliberately not a single combined URI.** Matching
Spark/Trino/DuckDB's own Iceberg REST support, and this project's
existing `MinioConfiguration`/`AdlsConfiguration.fromEnvironment()`
pattern: new `IcebergRestConfiguration` connects one REST catalog per
process from `ICEBERG_REST_*` environment variables (v1 limitation,
stated plainly — not a multi-catalog registry); the table reference
itself is just `icebergrest://<namespace>.<table>`, parsed via the real
`TableIdentifier.parse`. `IcebergTableFormat`/`IcebergTableWriter` each
gained one additive `(Catalog, TableIdentifier)` constructor —
`catalog.loadTable(identifier)` instead of `HadoopTables.load(path)`;
everything downstream (`normalize`/`appendBatch`) is unchanged, since
both already only operate on the resulting `Table`, not on how it was
obtained — the same thin-adapter shape proven twice already this
session. A new `TableLocation.fromStringWithEnvironmentConfig(String)`
(core, additive, zero risk to existing callers) derives where a loaded
table's data files actually live from its own reported `location()`,
reusing the exact remote-config-merging logic `TableResolver`/
`LabukDbFlightProducer` already had, since a catalog-loaded table has no
caller-supplied `TableLocation` to work from.

**Testing honesty, stated up front rather than discovered at
verification time:** no real or fake Iceberg REST catalog server existed
in this development environment when this was first written. The
format-agnostic `Catalog`-based core (both new constructors) is fully
tested against a real `InMemoryCatalog` — a genuine `iceberg-core`-bundled
`Catalog` implementation, not a mock — proving the actual read/write logic
correct for *any* catalog, `RESTCatalog` included, since they share one
interface. `IcebergRestConfiguration`'s env-var → property-map mapping is
unit-tested. The `icebergrest://` CLI/Flight routing is proven to *route
correctly* (tests assert the specific `ICEBERG_REST_URI`-required error a
REST-catalog reference reaches, not some unrelated Delta/path-based error
a misrouted request would produce instead).

**Update — the actual `RESTCatalog` network round-trip is now
live-verified too**, closing the one gap this section originally
flagged. `tabulario/iceberg-rest` (the standard open-source reference
Iceberg REST catalog server) was run locally and `IcebergRestConfiguration
.connect()` proven to genuinely speak the protocol end to end: connect,
create a namespace, create a table with a real schema and partition spec,
load it back and confirm both survived a real HTTP round trip, then drop
it (`IcebergRestCatalogLiveIntegrationTest`). Scoped deliberately to
metadata/wire-protocol operations, not a full write-then-read data round
trip — `IcebergTableWriter`'s data files are written directly by this
project's own JVM, and aligning that with a REST server's warehouse path
across this (Windows-host, Linux-container) development environment
without materializing cloud credentials into the container turned out to
be its own real constraint, not glossed over. The read/write logic itself
was already proven correct for any `Catalog` via `InMemoryCatalog` above;
what this adds is proof the wire protocol itself genuinely works.

**Update — DECIMAL type support is now done, in the shared `decode/`/
`encode/` codec both Delta and Iceberg already go through unchanged (not
a per-format change).** Chosen as the first of three remaining follow-up
items (checkpoint verification against real Spark output and a
remote+Iceberg OCC stress test are separate, later, infrastructure-
dependent passes) since it's self-contained and DECIMAL — money/prices —
is the single most common real-world type gap.

**Scope, stated plainly: Decimal128 only, precision ≤38.** `Decimal256`
(precision up to 76) is rejected with a clear error by the new
`decode/DecimalCodec`, not silently truncated or mishandled. Nested
`LIST<decimal>` and MAP/STRUCT remain separate, unstarted follow-ups.

**Four independent dispatch points needed a decimal check, confirmed via
direct reads — no single chokepoint exists (the same shape as the
existing INT96 precedent, which also touches four files):**
`ArrowSchemaMapper.toArrowType` (schema; signature widened from
`PrimitiveTypeName` to the full `PrimitiveType` since it's its own only
caller), `RowAppender.append` (value decode), `GroupPredicateEvaluator
.readValue` (residual-filter value), and `RowGroupPruner.toJavaValue`
(row-group min/max stats). Decimal is physically INT32, INT64,
FIXED_LEN_BYTE_ARRAY, or BINARY — all four already meant something else
in the existing per-dispatch-point switches — so every site needed an
early decimal check *before* its existing switch, not a new case folded
into it.

**A real pre-existing bug found during research, fixed as part of this:**
`RowGroupPruner.toJavaValue` had no decimal handling at all — an
INT32/INT64-backed decimal stat would have passed through as the raw
*unscaled* integer (e.g. unscaled `1050` compared directly against a
query literal, instead of the `10.50` it actually means at scale 2), and
a FIXED_LEN_BYTE_ARRAY/BINARY-backed one would have been mis-decoded as a
UTF-8 string. Both are fixed; proven by a dedicated
`ParquetToArrowReaderDecimalTest` case that asserts row-group pruning
makes the correct keep/skip decision against real decimal stats.

**Write narrow, read broad — the same split `Int96TimestampCodec`
established for legacy-vs-modern timestamps.** This project's own writer
(`ArrowToParquetWriter`) always encodes a decimal as a constant 16-byte
`FIXED_LEN_BYTE_ARRAY` regardless of actual precision — one simple
encoding path, not the minimal-bytes-for-precision optimization real
writers like Spark use. The read side isn't limited to that convention:
`DecimalCodec.fromBytes`/`fromUnscaledLong` between them decode a decimal
column backed by *any* of Parquet's four legal physical representations,
proven against a hand-built, non-this-project-written fixture (one column
per physical type) in `ParquetToArrowReaderDecimalTest`, not just a
round-trip of this project's own narrow write convention.

`PredicateEvaluation.coerce` needed **no changes** — confirmed by reading
the real coercion logic, not assumed — since it already coerces any
`Number` (`BigDecimal` included) into a `Double` when the predicate
literal is a `Double`, which is what `SqlQueryTranslator`/`PredicateCodec`
already produce for decimal-looking literals.

Verified with `DecimalCodecTest` (pure unit: round-trip encode/decode,
the >38-precision rejection, the scale-mismatch rejection),
`ParquetToArrowReaderDecimalTest` (real-world interop across all four
physical encodings, a residual-filter predicate, and the row-group-
pruning bug fix), and `ArrowToParquetWriterTest` (`encode/`'s first test
file: write + full write-then-read round trip, null handling, and the
>38-precision rejection at schema-build time). Full `labukdb-core` suite
re-run clean: 161/161 passing (146 pre-existing + 15 new), 0
failures/errors, 5 skipped — unchanged from baseline, confirming zero
regression.

**Update — `LIST<primitive>` write support is now done, closing a gap
this project had carried and documented honestly rather than papered
over.** Read support (`decode/NestedTypeDecoder`, five element types —
float, double, int32, int64, string) existed first, built for one
specific driver: *consuming* a pre-existing Delta table a Spark
ML/MLflow job already wrote with an `ARRAY<FLOAT>` embedding column.
Nothing in this engine's own write path ever needed to *originate* a
list column, so the symmetric write side was never built —
`ArrowToParquetWriter`'s own javadoc said so plainly. Status-tracking
conversation shorthand had drifted into "nested types: not started,"
which direct inspection of the actual code corrected — read was already
done and tested; only write was the real gap.

New write-side code, mirroring `NestedTypeDecoder`'s scope exactly (same
five element types, same 3-level encoding convention): `ArrowToParquetWriter
.buildListType`/`toListElementParquetType` (schema) and `appendListCell`/
`appendListElement` (value encode). Built via raw `GroupType`/`OriginalType`
construction, deliberately not the newer `Types.list(...)`/
`LogicalTypeAnnotation`-based builder, so the written file matches
exactly what `NestedTypeDecoder.isListType` checks for — the same
old-style `OriginalType.LIST` annotation `MessageTypeParser` already
produces for `NestedTypeDecoderTest`'s own hand-rolled fixture. A null
list element is represented by omitting that repeated group's `element`
field entirely, reusing the identical convention the read side already
expects.

`ArrowToDeltaSchema` needed a real shape change, not just a new case:
Delta's array type is a nested JSON object
(`{"type":"array","elementType":<type>,"containsNull":<bool>}`), not a
plain type-name string the way every other supported type is —
confirmed against the Delta Sharing protocol spec and delta-rs's own
schema handling, not assumed. `toDeltaTypeNode` now returns a `JsonNode`
(a `TextNode` for primitives, unchanged; an `ObjectNode` for `List`)
rather than a bare `String`, since the field's `"type"` value can
legitimately be either shape.

Verified with three new `ArrowToParquetWriterTest` cases (float-list
round trip, an empty-but-not-null list, a string-list round trip) and a
new `ArrowToDeltaSchemaTest` (previously untested directly) proving the
array-vs-string JSON shape and that `containsNull` reflects the
*element* field's nullability, not the list field's. Full regression:
179 (core) + 23 (format-iceberg) + 12 (cli) = 214 tests, 0
failures/errors, 27 skipped — unchanged from baseline.

Remaining, explicitly out of scope for this pass: nested `LIST<LIST<...>>`
(list of lists), MAP, STRUCT — a different problem shape (arbitrary field
nesting vs. one repeated element), not started.

**Update — Iceberg CREATE TABLE is now done, closing the most visible
asymmetry between the two formats: Delta already had `TableCreator`,
`TableMerger`, `Compactor`, and `Vacuum`; Iceberg had none of the four.**
This is the first of them — starting point chosen deliberately, since
merge/compaction/vacuum all assume a table already exists.

New `ArrowToIcebergSchema` (Arrow `Schema` → `org.apache.iceberg.Schema`)
mirrors `core.encode.ArrowToDeltaSchema`'s exact supported-type set —
Int32, Int64, Float, Double, Bool, Utf8, Timestamp, Decimal, and
`List<primitive>` — on the theory that a column this can't express
couldn't be written by `ArrowToParquetWriter` either, the same reasoning
`ArrowToDeltaSchema`'s own javadoc already uses.

**A real design mistake found and fixed before it shipped, not
assumed correct from memory:** the first version built every
`Types.NestedField`/`Types.ListType` with a placeholder field ID
(`0`) and planned to fix them up afterward via
`TypeUtil.assignFreshIds` — a pattern recalled as "the standard way,"
but never actually run. It failed immediately: `org.apache.iceberg
.Schema`'s own constructor eagerly builds and validates an id→name
index *at construction time*, before any after-the-fact reassignment
ever gets a chance to run — `new Schema(fields)` with duplicate
placeholder IDs throws `IllegalArgumentException: Multiple entries
with same key` before `assignFreshIds` is ever reached. Fixed by
threading one running `AtomicInteger` counter through the whole
recursive build instead, assigning each field's real, already-unique
ID *before* it's used — including a `List` field's separate element
ID — so the schema is valid the moment it's constructed, no second
pass needed.

New `IcebergTableCreator`, matching `IcebergTableWriter`'s own two
constructor shapes exactly, so a table created either way is
immediately usable by the existing, unmodified writer/reader: a
path-based overload (via `HadoopTables.create`) and a catalog-based
overload (via `Catalog.createTable`, proven against a real
`InMemoryCatalog`, the same honesty-preserving precedent the REST
catalog work established — any `Catalog` implementation shares one
interface). Partitioning is identity-transform only, matching the
Hive-style "exact value, no transform" partitioning `core.log
.TableCreator`/`CommitWriter` already use — Iceberg's richer
bucket/truncate/time-transform partitioning is a deliberate, separate,
later decision, not required for write-lifecycle parity with Delta.

**A second real, pre-existing gap found by actually exercising the new
code, not written defensively in advance:** `IcebergTableFormat
.normalize` unconditionally called `scan.snapshot().snapshotId()` —
fine for every existing test, since all of them appended at least one
file before ever calling `.latest()`. A freshly created table with zero
commits has no current snapshot at all (Iceberg's own convention,
genuinely different from Delta, where version 0 — metadata + protocol,
no data — always exists right after creation), so `scan.snapshot()`
returns `null` and the old code threw a bare `NullPointerException`
the instant `IcebergTableCreatorTest` tried to read back a freshly
created, never-appended-to table. Fixed by returning an empty
`NormalizedSnapshot` (version `-1`, mirroring Iceberg's own
"no current snapshot" sentinel) before ever calling `scan.planFiles()`,
rather than catching the NPE after the fact.

Verified with new `ArrowToIcebergSchemaTest` (every supported type,
plus a dedicated check that a `List` field's element gets a field ID
distinct from every top-level field's) and `IcebergTableCreatorTest`
(the real proof: create → append → read back, for both the path-based
and catalog-based constructors, an unpartitioned-table case, and a
table-already-exists rejection). Full regression: 179 (core) + 32
(format-iceberg, +9) + 12 (cli) = 223 tests, 0 failures/errors, 27
skipped.

Remaining, explicitly out of scope for this pass: Iceberg
merge/upsert, compaction, and vacuum — each a separate, later piece of
write-lifecycle parity, not started.

**Update — Iceberg's path-based (`HadoopTables`) multi-writer OCC is
confirmed broken on real MinIO, root-caused precisely, not just
documented as a risk.** A real 8-separate-process stress test
(`IcebergTableWriterConcurrentMinioIntegrationTest`) found 6 of 8
concurrent appends silently lost — every writer reported success, only 2
of the 8 files ended up visible in the final table.

**Took three attempts to get a methodologically sound test, each failure
itself a real, worthwhile finding:**
1. First attempt used threads, not separate processes — a false negative.
   Bytecode inspection of `iceberg-core` found `HadoopTables`' default
   `LockManager` is `InMemoryLockManager`, a plain `HashMap` living inside
   one JVM. Threads sharing one process share that lock and get perfectly
   serialized by it — never exercising the real storage-layer risk at all.
2. The process-based rewrite then deadlocked: `Process.waitFor()` was
   called before draining each child's stdout pipe, and a worker's routine
   SLF4J/log4j/Hadoop startup logging filled the OS pipe buffer before it
   ever printed its one real result line — both sides blocked forever.
   Fixed by redirecting each worker's output to a file instead of a pipe.
3. The fixed version then crashed immediately: `ProcessBuilder` doesn't
   inherit the parent JVM's own command-line flags (only its environment),
   and this project's root `pom.xml` sets `--add-opens` for Arrow's
   off-heap memory access via Surefire's `argLine` — a plain child `java`
   invocation lacks it, so every worker died on Arrow's `MemoryUtil` init
   before ever touching MinIO. Fixed by passing the flag explicitly.

**Root cause, traced through `iceberg-core`'s real source
(`HadoopTableOperations.renameToFinal`), not assumed:**
1. It acquires a `LockManager` lock first — but as found above, the
   default is per-JVM-only and provides zero protection across genuinely
   separate processes.
2. It then checks `fs.exists(dst)` — a classic check-then-act race with no
   real lock behind it.
3. It then calls `fs.rename(src, dst)`, with the library's own code
   comment stating *"this rename operation is the atomic commit
   operation"* — true on HDFS/local disk (a metadata-only pointer swap),
   false on S3A, where rename is copy-then-delete: two separate network
   calls, no compare-and-swap, no rejection if the destination was already
   written. Every racing writer's rename can return `true` with no
   conflict ever detected — last physical writer wins, silently, with no
   exception for anyone to retry on. (`SnapshotProducer`'s own retry loop
   only fires on an actual `CommitFailedException` — a writer that hits
   this exact race never gets one.)

This matches Iceberg's own documentation, which already calls
`HadoopTables`/`HadoopCatalog` unsafe for concurrent use on S3-compatible
storage. Iceberg's own documented answer is "use a real catalog instead"
(REST, Hive Metastore, Glue — whose commit is backed by an actual database
transaction, not an object-store rename), which this project already
supports via `IcebergTableWriter(Catalog, TableIdentifier)`, but that path
is not yet live-verified for concurrency either.

**Update — a lock-based mitigation has since been built for this path**,
smaller than Delta's `LockProvider` since Iceberg already has the
extension point Delta's log format doesn't: `org.apache.iceberg.LockManager`
is a tiny, already-pluggable interface (`acquire`/`release`/`initialize`),
activated by one Hadoop `Configuration` property
(`iceberg.tables.hadoop.lock-impl`, confirmed via `javap -constants` on
`HadoopTables`) — no change to Iceberg's own commit code needed. The new
`DynamoDbLockManager` implements it with a lease-based acquire/release
(`attribute_not_exists(lockKey) OR expiresAt < :now`), reusing the same
`DynamoDbClient` plumbing as Delta's lock. The exact same 8-separate-
process scenario above, re-run with the lock engaged
(`IcebergTableWriterConcurrentMinioLockedIntegrationTest`), lands all 8
commits — live-verified against real MinIO + `amazon/dynamodb-local`.
Full details in `DynamoDbLockManager`'s own javadoc.

**Also added — a proportionate warning when a remote writer has no lock
configured** (`RemoteWriteSafetyWarning`, used by both `CommitWriter` and
`IcebergTableWriter`): the engine can't detect an actual concurrent
writer, only risky configuration, so it logs a strong warning for an
explicit custom S3 endpoint (MinIO-shaped) and a lighter note for any
other remote location, and stays silent for local tables or once a lock
is engaged.

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
   for remote too** (`FlightRemoteAggregationMinioIntegrationTest`).
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
11. ✅ **SQL query layer, on the CLI.** Done, see above —
    `core/query/SqlQueryTranslator` (built on `jsqlparser`) plus the new
    shared `core/execution/CombinedQueryExecutor` both `labukdb-cli` and
    `labukdb-flight` now call, wired into the CLI's new `--sql` flag.
    SQL-over-Flight remains a deliberate, separate follow-up.
12. ✅ **Iceberg reads wired into `cli`/`flight`.** Done, see above —
    `format/DetectedFormat` auto-detects Delta vs. Iceberg per table
    location; `--table`/`--sql`/Flight all reach Iceberg tables now, not
    just a direct Java call to `IcebergTableFormat`. Delta's own query
    path is completely unchanged.
13. ✅ **Iceberg writes (append-only), plus CREATE TABLE.** Done, see
    above — new `IcebergTableWriter`, reachable via `DoPut` and the
    embedded library. **Update — `IcebergTableCreator` is now done too**,
    both path-based and catalog-based, closing the write-lifecycle gap
    with Delta's own `TableCreator`; see its own entry further down for
    detail. Iceberg merge/upsert, compaction, and vacuum remain unbuilt;
    no CLI write subcommands for either format.
14. ✅ **Iceberg REST catalog integration.** Done, see above — new
    `IcebergRestConfiguration`/`IcebergRestLocation`, `icebergrest://`
    table references reachable via `--table`/`--sql`/Flight (reads and
    `DoPut` writes), plus `(Catalog, TableIdentifier)` constructors on
    `IcebergTableFormat`/`IcebergTableWriter` usable directly as a
    library for *any* `Catalog` implementation, not just REST. Hive
    Metastore/Glue/Nessie remain unbuilt; implemented but not
    live-verified against a real REST server (see above for why).
15. ✅ **Renamed `DeltaFlightProducer` → `LabukDbFlightProducer`.** The
    class now serves both Delta and Iceberg (path- and catalog-based),
    reads and writes — "Delta" in the name had become actively
    misleading. Renamed via `git mv` to preserve history; every call
    site (`LabukDbServerApp`, all `labukdb-flight` tests) and every
    doc/javadoc mention across `labukdb-core`/`labukdb-cli` updated in
    the same pass, confirmed via a repo-wide grep for the old name
    afterward (none left except the class's own javadoc explaining the
    rename). A pre-existing stale duplicate javadoc block on
    `resolveTableRoot` (two `/** */` comments back to back, the first
    superseded by the second) was also cleaned up while touching this
    file, and the class-level javadoc's "DoPut/DoAction/DoExchange out
    of scope" line — long stale, `DoPut` has been fully implemented for
    several passes now — was corrected at the same time.

**Known, pre-existing flaky test — not caused by the work above, root
cause still genuinely open.** `labukdb-flight`'s `FlightIntegrationTest`
intermittently fails (~1-in-4 observed) with `IllegalStateException:
Memory was leaked by query` during `@AfterEach`'s `allocator.close()`.
Confirmed not specific to any one test — it's hit a pure-read test
(`partitionPruningAndResidualFilter...`, untouched by this session's
write-path changes) as often as a `DoPut` one, and has now also been
observed once on the new `FlightIcebergIntegrationTest` (passed cleanly
on every other run, standalone and full-suite, with no resource-handling
difference from the already-stable `FlightAggregationIntegrationTest` it
mirrors) — reinforcing that this is suite-wide timing/load-dependent
flakiness, not tied to any one test's own code.

**Correction to an earlier note in this doc:** this was previously
attributed to Arrow's own [ARROW-4448](https://issues.apache.org/jira/browse/ARROW-4448)
(same ticket as [apache/arrow#21006](https://github.com/apache/arrow/issues/21006)
post-migration), "explicitly concluded to be a flake." On closer reading
that's wrong: the ticket was **resolved as fixed**, not dismissed, via
[PR #3805](https://github.com/apache/arrow/pull/3805) targeting Arrow
0.13.0 (2019). This project pins Arrow **16.1.0** — the fix shipped many
major versions before that, so this can't be the same bug recurring; the
match was only the generic leak-detector message text, not a confirmed
shared root cause. The actual cause here is unverified and still open —
not assumed to be Arrow's fault or this project's own code without
actually reproducing and diagnosing it (e.g. a heap-dump/allocator-debug
run under a loop that reproduces the ~1-in-4 rate).

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
| Remote atomic commits (S3/MinIO/ADLS) | `HadoopLogStorage.createCommitFile`, Hadoop bumped to 3.4.3 | ✅ Implemented; **MinIO real-infra verified for a single sequential writer only** — ⚠️ confirmed NOT atomic under genuine concurrent writers on this real deployment, see the OCC table above. S3/ADLS implemented-but-unverified either way. |
| `format/TableFormat` + `DeltaTableFormat` | Additive SPI prep | ✅ Done, tested standalone |
| **Multi-format — `labukdb-format-iceberg`** | `IcebergTableFormat`, using `iceberg-core:1.12.0` (not hand-rolled) | ✅ **Done** — read-only, path-based (`HadoopTables`, no catalog service). Validated the `TableFormat` shape as intended: surfaced two real mismatches (`atVersion`'s Delta-sequential assumption vs. Iceberg's non-sequential `snapshotId`; `schemaString`'s Delta-JSON shape vs. Iceberg's typed `Schema`) — named, not silently papered over. Also empirically proved `decode/ParquetToArrowReader` is format-agnostic: reads an Iceberg-committed file with zero changes. Tested against a self-built fixture table (`iceberg-core`'s own create API), not a Spark/Trino-written one — same caveat `CheckpointReader` carries for Delta checkpoints. **Remote (MinIO) now live-verified too** — the constructor was always location-agnostic (`HadoopTables` resolves via Hadoop's own `FileSystem`), just never actually run against a live cluster before; Iceberg's own file paths are absolute, not relative like Delta's, confirmed from how the fixture-building code registers them. |
| `flight/DoPut` | Ingest endpoint | ✅ Done — local and remote, real `pyarrow.flight` client (local) and real `FlightClient` against live MinIO (remote) both verified |
| `TableFormat` wired into `planner`/`flight`/`cli` | Still deferred | Two implementations now exist and have already informed the interface (see mismatches above) — wiring itself still not done |
| Read scale-out | Self-describing Flight tickets (encode the scan directly in the ticket bytes instead of a `TicketRegistry` lookup), stateless server replicas behind a load balancer | Not started — cheap once started, no new distributed-systems machinery needed since snapshot resolution is already stateless against shared object storage |
| CREATE TABLE | `TableCreator`, local + remote | ✅ **Done** — library capability only, not exposed over Flight/CLI yet |
| Multi-writer OCC — local disk | Retry + conflict detection in `CommitWriter` | ✅ **Done** — real 10-thread + real-table concurrency tests, see "Concurrent writes" above |
| Multi-writer OCC — remote, no lock (S3/MinIO/ADLS) | `commitWithRetry`'s plain create-if-absent path | 🔴 **Confirmed broken on at least one real MinIO deployment** — silent lost updates under genuine concurrency (`CommitWriterConcurrentMinioIntegrationTest`, left failing on purpose as a standing proof). This is the real-cloud-likely-fine, self-hosted-storage-risky case — see the lock row below. |
| Multi-writer OCC — remote, with lock | New opt-in `LockProvider`/`DynamoDbLockProvider`, `CommitWriter.forLocation(location, lockProvider)` | ✅ **Done, live-verified** — same scenario that fails above now passes with the lock engaged (`CommitWriterConcurrentMinioLockedIntegrationTest`, real MinIO + `amazon/dynamodb-local`); crash-recovery proven by a deterministic local test (`CommitWriterLockRecoveryTest`). Scoped as a mitigation for non-compliant S3-compatible storage specifically, not a universal multi-cloud requirement — see "Concurrent writes" above. |
| **Distributed ingestion fleet** | Router + sharded Ingesters + a durable distributed WAL (not local-disk-per-node) + a catalog service tracking which node owns which table's live buffer — IOx's Router/Ingester split | **Planned, not yet designed.** Genuinely the largest remaining piece — closer to a distributed-systems project than an extension of the single-node write path, and needs its own dedicated design pass once v1 core is real |
| **Distributed query execution** | Splitting one query's work across multiple compute nodes (join/aggregate pushdown across a fleet, not just client-side fan-out over `FlightEndpoint`s) | **Planned, not yet designed.** Needs a real distributed query planner; revisit once `execution/` exists single-node and its limits are actually felt |

---

## Known test flakiness — `FlightIntegrationTest`

**A known, pre-existing flaky test — root cause still open, not Arrow's.**
`FlightIntegrationTest` intermittently fails (~1-in-4 observed) with
`IllegalStateException: Memory was leaked by query` during
`allocator.close()` in teardown — confirmed not tied to any specific code
path (it hits read-only tests as often as write ones, and has now also
been observed once on `FlightIcebergIntegrationTest`, a brand-new test
with no resource-handling difference from the already-stable
`FlightAggregationIntegrationTest` it mirrors — passed cleanly on every
other run, standalone and full-suite, reinforcing that this is suite-wide
timing/load-dependent flakiness, not a defect in any one test). Earlier
notes in this project cited Arrow's own
[ARROW-4448](https://issues.apache.org/jira/browse/ARROW-4448) /
[apache/arrow#21006](https://github.com/apache/arrow/issues/21006) (the
same ticket — ARROW-4448 is its old JIRA id, #21006 its GitHub-migration
id) as an explanation; that was wrong on closer reading. That issue was
**resolved as fixed** (not dismissed as an unfixable flake) via
[PR #3805](https://github.com/apache/arrow/pull/3805), targeting Arrow
**0.13.0** in 2019. This project pins Arrow **16.1.0** — the fix already
shipped many major versions before the version this project depends on,
so it can't be the same recurring bug; the resemblance is only the
generic leak-detector message, not the root cause. The actual cause here
is genuinely unverified and still needs real investigation (e.g. a
heap-dump/leak-detector run under the loop that reproduces it), not
assumed to be either an Arrow bug or this project's own code.

---

*This document and its diagrams describe a plan, not current behavior.
Update both the `.puml` source and this file together as the design
changes — don't let them drift the way `ARCHITECTURE.md` briefly did
about nested-type support.*
