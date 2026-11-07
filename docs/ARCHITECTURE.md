# labukDB — Architecture

Diagrams live under [`docs/diagrams/`](diagrams/) — Mermaid, rendered
natively by GitHub, no plugin needed. See [`docs/README.md`](README.md)
for the index.

---

## What this is

A from-scratch Java lakehouse engine that reads and writes both
**Delta Lake** and **Apache Iceberg** tables — the transaction log,
pruning, Parquet encode/decode, and the write path (append, upsert/MERGE
by key, compaction, vacuum, table creation, multi-writer concurrency)
are implemented directly against each format's real on-disk protocol,
not delegated to `delta-rs`, `delta-kernel`, DuckDB, or Spark. The one
deliberate exception is Iceberg's own commit/manifest machinery:
`iceberg-core` (the official Apache library) handles manifest-list/
manifest-file parsing directly, rather than hand-rolling Avro parsing
the way `log/` hand-rolled Delta's JSON log — Iceberg's metadata surface
is materially larger than Delta's, and re-deriving it from scratch
wasn't judged worth the effort relative to what it would teach.

Usable as an embedded Java library with no server (the DuckDB model), or
as a standalone Arrow Flight server any language with a Flight client
binding can connect to (the Postgres model) — both from the same
`labukdb-core` engine. A SQL layer sits on top of both, with a real
two-table join.

## Why this shape

Three decisions drive the whole layout:

1. **Stay Delta-protocol-compatible rather than invent a new format.**
   labukDB writes the same `_delta_log` protocol it reads — a table it
   writes stays readable by Spark, DuckDB, `delta-rs`, or Databricks, and
   a Spark-written table stays readable by labukDB. This turns "build a
   database" into "build a writer for a log format already understood as
   a reader."
2. **One `TableFormat` abstraction over both formats.** `format/TableFormat`
   normalizes a Delta or Iceberg snapshot to the same shape (schema,
   partition spec, file list), so `planner/`, `decode/`, and the SQL
   engine serve both without caring which one they're looking at.
3. **Keep the embeddable core dependency-light.** `labukdb-core` has no
   Flight/gRPC dependency — embedding it in-process doesn't pull in
   Netty/gRPC just because the project also offers a server mode.

## Two access modes, one engine

| Mode | Shape | Analogous to |
|---|---|---|
| Embedded library | Add `labukdb-core` (+ format modules) as a Maven dependency, call the Java API in-process | DuckDB — the engine lives inside your JVM process |
| Flight server | Run `labukdb-server-app`, connect from any language with an Arrow Flight binding | Postgres/MySQL — a separate process, network protocol |

## Modules

| Module | Packages | Depends on | Produces |
|---|---|---|---|
| `labukdb-core` | `log/`, `planner/`, `decode/`, `encode/`, `execution/`, `format/`, `query/`, `logical/`, `catalog/`, `storage/` | — | embeddable library jar |
| `labukdb-format-iceberg` | `IcebergTableFormat` (reads); `IcebergTableWriter`/`IcebergTableMerger`/`IcebergCompactor`/`IcebergVacuum` (writes); path-based (`HadoopTables`) or catalog-based (`Catalog`/`TableIdentifier`, including REST catalogs via `IcebergRestConfiguration`) | `labukdb-core`, `iceberg-core` | library jar — optional for a pure `labukdb-core` embedder, required by `cli`/`flight` |
| `labukdb-flight` | `flight/` — `GetFlightInfo`/`DoGet`/`DoPut`/`ListFlights` | `labukdb-core`, `labukdb-format-iceberg` | library jar |
| `labukdb-server-app` | bootstraps `LabukDbServerApp` | `labukdb-core`, `labukdb-flight` | shaded standalone server jar |
| `labukdb-cli` | `cli/` — the `labukdb` binary | `labukdb-core`, `labukdb-format-iceberg` | shaded CLI executable |

The dependency direction is one-way: `labukdb-core` never depends on
`labukdb-format-iceberg`. Format *detection* (`DetectedFormat`) lives in
`core`, since it only needs `TableLocation`; actually *constructing* an
`IcebergTableFormat` happens in `cli`/`flight`, the first layer that can
see both modules.

## The engine, package by package

**`log/`** — Delta's commit log. `CommitLogReader` parses one commit
file into actions (`add`/`remove`/`metaData`/`protocol`);
`SnapshotManager` replays them into a `Snapshot` — the live file set and
schema at one version, cached incrementally so a streaming table doesn't
need a full replay on every query. On the write side: `CommitWriter`
(atomic versioned commits with optimistic-concurrency retry — see
[Concurrent writes](#concurrent-writes)), `TableCreator` (writes a
brand-new table's version-0 commit), `TableMerger` (upsert/MERGE by key),
`Compactor` (bin-packs a partition's small files into one, as a
non-data-changing commit), and `Vacuum` (deletes the bytes of files a
past commit already removed, once they're past a retention window —
7 days by default, matching real Delta's own).

**`planner/`** — decides which files can be skipped without opening
them: partition pruning (exact match on partition values already in the
log, no I/O) and file-stats pruning (range check on the min/max stats
the log already carries, no I/O). Fail-open throughout — anything it
can't decide, it keeps, since wrongly skipping a file loses real data
and wrongly keeping one only wastes a read.

**`decode/`** — Parquet bytes to Arrow vectors. Per surviving file:
row-group pruning via the Parquet footer (opens the file, footer only),
then decodes only the requested columns while applying the residual
filter row-by-row, fused into one pass — a row that fails the filter is
never materialized into an Arrow vector at all. Handles nested types
(`LIST`/`MAP`/`STRUCT`, any nesting depth), `DECIMAL`, and Spark's legacy
`INT96` timestamp encoding.

**`encode/`** — the write-side mirror of `decode/`: Arrow to Parquet, and
an Arrow `Schema` to each format's own schema representation
(`ArrowToDeltaSchema`, `ArrowToIcebergSchema`).

**`execution/`** — the operators that run once a query needs more than a
plain filtered scan: `AggregateOperator` (`GROUP BY` plus `SUM`/`COUNT`/
`MIN`/`MAX`/`AVG`), `SortLimitOperator` (`ORDER BY`/`LIMIT`, built on
`arrow-algorithm`'s sort primitives rather than hand-rolled comparison
logic), `HashJoinOperator` (a two-table `INNER` equi-join — classic hash
build/probe, with a true fan-out on a duplicate build-side key and
standard SQL null-key semantics), and `CombinedQueryExecutor` (decode,
concatenate, then optionally aggregate and optionally sort/limit — what
any query that can't be answered file-by-file needs, since a `GROUP BY`,
a global `ORDER BY ... LIMIT`, and a join all need to see every
surviving row before producing a result).

**`format/`** — the `TableFormat` interface (`latest()`/`atVersion()`/
`atTimestamp()`, returning a normalized snapshot) that both
`DeltaTableFormat` and `IcebergTableFormat` implement, plus
`DetectedFormat`, which auto-detects which one a table location holds by
probing for `_delta_log/` versus `metadata/`.

**`query/`** — `SqlQueryTranslator` parses real `SELECT` text (via
`jsqlparser`, not a hand-rolled grammar — parser theory isn't one of this
project's own from-scratch learning goals) straight into the engine's
own already-executable structures: `Predicate`, `AggregateSpec`,
`SortKey`, and a join clause.

**`logical/`** — a `LogicalPlan` tree (`TableScan`/`Filter`/`Aggregate`/
`Sort`/`Limit`/`Project`/`Join`) and a small rule-based optimizer
(`PushFilterIntoScan`, `PushProjectIntoScan`) that absorbs a `WHERE`
clause directly onto its scan and prunes columns down to what a query
actually needs. One resolution path serves Delta, Iceberg, and
REST-catalog Iceberg alike — including a join, where each side resolves
completely independently, which is what lets a Delta table join an
Iceberg table as a consequence of that independence rather than a
specifically-built feature.

**`catalog/`** — `TableCatalog` maps a short name to a table location,
persisted to `~/.labukdb/catalog.json`. No format tag is stored per
entry; each consumer detects format at query time via `DetectedFormat`,
not at registration time, so a Delta table and an Iceberg table can be
registered side by side under different names.

**`storage/`** — `TableLocation` resolves local disk, S3/MinIO
(`s3a://`), and ADLS Gen2 (`abfss://`) through one entry point, by URI
scheme — see [Storage backends](#storage-backends).

## Write path: it's an LSM tree, at file granularity

`CommitWriter` + `Compactor` are structurally an LSM tree, just applied
to whole files instead of individual rows — the same shape Delta,
Iceberg, Hudi, and IOx (underneath InfluxDB 3.0) all share:

| Classic LSM (RocksDB/Cassandra) | labukDB (file-level LSM) |
|---|---|
| Memtable (in-memory sorted writes) | `WriteBuffer` |
| WAL (durability before flush) | `WriteBuffer`'s WAL |
| SSTable (immutable file) | a committed Parquet file |
| Compaction (merge small SSTables) | `Compactor` |
| Tombstone / merge-on-read | `remove` actions in the commit log |

**Why `WriteBuffer` earns its place**: without it, every write would
flush straight to one new Parquet file plus one new commit. Under
frequent small writes — the exact pattern a time-series/metrics
ingestion workload produces — that's a tiny file and a log entry per
call, and `Compactor` would be permanently fighting a losing battle
instead of doing periodic cleanup. `WriteBuffer` buffers per table in
memory, backed by a WAL for crash durability, and only flushes to
`CommitWriter` when a size or time threshold is hit (128 MB or 60
seconds, whichever comes first). It also serves reads directly — a query
sees its own very recent, not-yet-flushed writes by checking the buffer
alongside committed files, the classic LSM "check the memtable, then the
SSTables" read path.

`WriteBuffer`'s WAL stays local-disk-only even for a remote table — it's
server-internal durability staging, never part of the Delta protocol or
visible to any other reader, so redirecting it to S3/MinIO would trade
away the latency/cost reason it exists in the first place.

## Concurrent writes

labukDB inherits Delta's own answer to concurrency rather than inventing
one — optimistic concurrency control (OCC):

1. A writer reads the latest version `V`.
2. Builds its change set (add/remove actions) against that snapshot.
3. Attempts to commit as `V+1` via an **atomic create-if-absent** write
   of `_delta_log/<V+1>.json` — this must fail, not silently overwrite,
   if the file already exists.
4. Success: done. Failure (someone else won the race for `V+1`):
   re-read the new latest version and check whether the winning commit's
   actions actually conflict with this writer's read/write set. No real
   conflict: rebase and retry as `V+2`. Real conflict: fail the
   transaction and surface it to the caller.
5. Bounded retries with randomized backoff, to avoid livelock under
   contention.

The algorithm is mechanical; the actual risk is step 3's atomic
create-if-absent, and it is **not uniform across storage backends**:

| Backend | Atomic create-if-absent | Status |
|---|---|---|
| Local disk | `Files.createFile(path, CREATE_NEW)` | Reliable — a real atomic filesystem operation |
| Real AWS S3 | Conditional PUT (`If-None-Match: *`), native since late 2024 | Implemented; not independently verified against a real AWS endpoint from this project |
| MinIO (this project's actual S3-compatible deployment) | Same code path as S3 | **Confirmed not atomic under real concurrency.** An isolated 8-thread test calling the underlying create-if-absent operation simultaneously on the identical key found all 8 "succeeded," zero conflicts detected |
| ADLS Gen2 | Create-without-overwrite-first, native | Implemented; not independently verified against a real ADLS endpoint, and given the MinIO finding, not assumed correct without the same kind of test |

Real Delta Lake OSS historically needed an external coordinator (a
DynamoDB table) for S3 specifically, before S3 gained native conditional
writes — this project rediscovered exactly why, on a real third-party
S3-compatible store. The mitigation is an **opt-in external lock**
(`LockProvider`, with `DynamoDbLockProvider` as the shipped
implementation), engaged via `CommitWriter.forLocation(location,
lockProvider)` — the no-lock `CommitWriter.forLocation(location)` is
unchanged and stays the default. The lock's recovery algorithm mirrors
real Delta's own `BaseExternalLogStore` approach: a commit claims an
incomplete lock entry before writing, writes the file, then marks the
entry complete; the next writer recovers any stuck entry for the version
it's about to attempt before proceeding. Validated against real MinIO
and a real DynamoDB-compatible backend — the same concurrent-writer
scenario that loses updates without the lock passes cleanly with it
engaged, for plain appends, `TableMerger`, and `Compactor` alike.

Iceberg's own writer has the identical class of bug on the same storage:
`iceberg-core`'s default `LockManager` is a per-JVM in-memory map, and
Iceberg's rename-based commit isn't atomic on S3A — real concurrent
writers can both "succeed" with no conflict detected. The mitigation is
the same shape: an opt-in `DynamoDbLockManager` implementing Iceberg's
own `LockManager` extension point, validated against real MinIO the same
way.

**Scope, stated precisely**: this is a mitigation for non-compliant
S3-compatible object storage, not a universal multi-cloud requirement.
Real AWS S3, real Google Cloud Storage, and real Azure Blob Storage/ADLS
are all documented as providing atomic conditional writes natively — a
real cloud deployment should likely never need to configure a
`LockProvider` at all. It exists for self-hosted or non-compliant
storage, where this project found the gap is real.

**Known limitation, not solved**: recovery is write-path-only — the next
writer repairs a stuck prior entry before proceeding, not a read-path
proactive recovery. A table abandoned mid-write and never written to
again before the lock entry's TTL expires stays unrecovered (real
Delta's own design carries the identical limitation). Every writer of a
given table must also agree on using the same `LockProvider`, or none at
all.

## SQL engine

A `SELECT` runs through a real logical-plan intermediate representation,
not string-matched directly against the request: `SqlQueryTranslator`
parses the text, `LogicalPlanBuilder` builds a `LogicalPlan` tree
(`TableScan`/`Filter`/`Aggregate`/`Sort`/`Limit`/`Project`, plus `Join`
when two tables are involved), a rule-based optimizer absorbs filters
onto their scan and prunes columns, and `LogicalPlanExecutor` resolves
the optimized plan into files to scan — one path for Delta, Iceberg, and
REST-catalog Iceberg alike.

**Supported today**: `SELECT <col> | <AGGFUNC>(<col>) AS <alias> FROM
<table> [WHERE ...] [GROUP BY ...] [ORDER BY ...] [LIMIT n]`, with
`=`/`!=`/`<`/`<=`/`>`/`>=`/`IN`/`LIKE`/`AND`/`OR` predicates and
`COUNT`/`SUM`/`AVG`/`MIN`/`MAX` aggregates — and a real two-table
`INNER` equi-join, including combined with `GROUP BY`/`ORDER BY`/
`LIMIT` on the joined result. Every column reference must be
table-qualified once a join is present. Each side of a join resolves
independently, through the same per-format detection a single-table
query uses — which is why a Delta table can join an Iceberg table with
no format-specific join code.

**Not supported**, stated as current scope boundaries: `LEFT`/`RIGHT`/
`FULL`/`CROSS` joins, non-equi join conditions, three-or-more-table
joins, cost-based build/probe side selection (the right table is always
the hash-build side), subqueries, computed `SELECT` expressions (every
item must be a bare column or one of the five aggregate functions — no
`a + b`, no `CASE`, no scalar functions), window functions, `UNION`, and
generic `NOT`/`IS NULL` (`Predicate` has no generic negation by design).
`COUNT(*)` specifically is rejected in favor of `COUNT(column)`
(non-null values of that column), matching the aggregate operator's own
semantics rather than introducing a second meaning for `COUNT`.

## Type system

`DECIMAL` (`Decimal128`, precision ≤ 38), the standard numeric/string/
boolean primitives, and `LIST`/`MAP`/`STRUCT` nested to any depth, read
and write, on both formats — `Types.StructType`/`Types.MapType` on the
Iceberg side, self-similar nested JSON on the Delta side. The leaf
primitive kinds a nested position bottoms out in are float, double,
int32, int64, and string; `DECIMAL`/timestamp/boolean leaves *inside* a
nested position are the one named gap — not wired, to avoid a schema
that builds but a reader that can't actually decode it.

## Storage backends

Local disk, S3/MinIO, and ADLS Gen2 all resolve through `TableLocation`
by URI scheme — no config file; the CLI and Flight server read connector
settings from environment variables automatically whenever a location
uses a remote scheme.

| Backend | Environment variables | Verification |
|---|---|---|
| Local disk | *(none)* | Exercised throughout |
| MinIO | `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` | **Live-verified** — the only remote backend actually stress-tested against a real deployment, across reads, writes, concurrency, the SQL engine, and joins |
| Real AWS S3 | *(none specific — falls back to Hadoop's default credential chain)* | Implemented, not independently verified against a real AWS endpoint |
| ADLS Gen2 | `ADLS_STORAGE_ACCOUNT`, `ADLS_AUTH_TYPE`, matching key/OAuth vars | Implemented, not independently verified against a real ADLS endpoint |
| Iceberg REST catalog | `ICEBERG_REST_URI`, plus warehouse/auth vars | Live-verified against a real `tabulario/iceberg-rest` server plus real MinIO storage, full write-then-read round trip |
| Remote OCC lock (opt-in) | `DYNAMODB_LOCK_TABLE`, `DYNAMODB_REGION`, `DYNAMODB_ENDPOINT`, `DYNAMODB_ACCESS_KEY`/`_SECRET_KEY` | Live-verified against both real AWS DynamoDB semantics and a self-hosted `amazon/dynamodb-local` container |

## Known scope boundaries

Stated plainly, as the current edges of the system rather than a
prioritized backlog:

- **Single process, single node.** No distributed query execution, no
  distributed ingestion fleet. Everything in this codebase runs on one
  machine.
- **Checkpoint reading only, and only against a self-built fixture** —
  not yet proven against a real Spark-produced checkpoint, which makes
  it the highest-risk, least-verified code in the project. Checkpoint
  *writing* doesn't exist.
- **No SQL-over-Flight** — a Flight client builds the JSON query shape
  directly; SQL text is a CLI-only entry point today.
- **No authentication or access control** on the Flight server.
- **`DoAction`/`DoExchange`** are unimplemented on the Flight surface —
  no CREATE TABLE RPC, no push-based tailing.
- **No persistent catalog beyond the local JSON file** — no Hive
  Metastore, Glue, or Nessie integration.
- **No cost-based query optimization** — two rule-based rewrites
  (filter pushdown, column pruning), nothing resembling a real
  statistics-driven planner.
