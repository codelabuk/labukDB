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
| `labukdb-format-iceberg` | *(new)* `IcebergTableFormat`, delegating manifest parsing to `iceberg-core` | `labukdb-core`, `iceberg-core` | embeddable library jar (optional) |
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
- **`format/Compactor` and `format/Vacuum`** — bin-pack small files into
  an `OPTIMIZE`-style commit, and remove orphaned file bytes past a
  retention window. Needs the same concurrency-safety rigor as
  `CommitWriter` itself, since it rewrites files a reader or another
  writer might be mid-snapshot against.
- **`execution/`** — `FilterOperator` (formalizes the existing residual-
  predicate pass), `AggregateOperator` (`GROUP BY` + `SUM`/`COUNT`/
  `MIN`/`MAX`/`AVG`), `SortLimitOperator`. This is the piece that was
  missing to call this a "database" rather than a scan gateway — not a
  general SQL optimizer, just enough for real workloads.

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
| Local disk | `Files.createFile(path, CREATE_NEW)` | Trivial, already atomic |
| S3 | `If-None-Match: *` conditional PUT | Native only since 2024 — whether `hadoop-aws`'s `S3AFileSystem` exposes this cleanly, or whether this needs the raw S3 SDK instead, is **unverified** |
| MinIO | Depends on S3 API version emulated | **Unverified** |
| ADLS Gen2 | Lease / `If-None-Match` via the `abfs` driver | **Unverified** |

Real Delta Lake OSS historically needed an external coordinator (a
DynamoDB table) for S3 specifically, before S3 gained native conditional
writes. This project would hit the same problem if it assumed otherwise
without checking.

**Recommendation:** scope labukDB v1 to **single-writer-per-table** — no
concurrent-commit races, because only one process ever writes a given
table at a time. Still a complete, useful database. This defers true
multi-writer OCC (and the per-backend verification it depends on) to v2,
once each backend's atomic-write primitive is actually confirmed rather
than assumed.

## Build order

Not all five new subsystems carry equal risk — ordered to get something
demoable early and defer the highest-risk, least-verified piece until the
foundation under it is proven:

1. **Single-writer commit path, no `WriteBuffer` yet** — "append one
   commit atomically, read it back correctly," one Parquet file per
   write. Proves `ArrowToParquetWriter` and commit-JSON construction
   before anything else adds complexity on top.
2. **`WriteBuffer`** — insert the memtable/WAL layer between `DoPut` and
   (1), so file count stops being one-per-write. Do this *before* wiring
   `DoPut` into the public API, not after — retrofitting a buffer under
   an API clients already depend on is strictly harder.
3. **`DoPut` on Flight** — wires the ingest entry point to (1) + (2).
4. **Compaction / vacuum.**
5. **`execution/`** — independent of the write path, can happen in
   parallel with 1–4.
6. **Multi-writer OCC** — deferred, per the recommendation above, until
   the per-backend atomic-write verification in
   [Concurrent writes](#concurrent-writes) is actually done. v1 ships
   single-writer-per-table instead.

---

*This document and its diagrams describe a plan, not current behavior.
Update both the `.puml` source and this file together as the design
changes — don't let them drift the way `ARCHITECTURE.md` briefly did
about nested-type support.*
