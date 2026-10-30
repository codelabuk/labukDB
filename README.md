# labukDB

[![CI](https://github.com/codelabuk/lakehouse-flight/actions/workflows/ci.yml/badge.svg)](https://github.com/codelabuk/lakehouse-flight/actions)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

**A from-scratch Delta Lake + Iceberg database — reads and writes, local
or remote, embeddable or served over Apache Arrow Flight.**

No `delta-rs`, no `delta-kernel`, no DuckDB/Spark in the path: the
transaction log, pruning, Parquet encode/decode, and the write path
(append, upsert/MERGE by key, compaction, vacuum, multi-writer
concurrency, table creation) are all implemented here, one small and
independently testable layer at a time. It stopped being read-only a
while ago — append, upsert/MERGE, compaction, vacuum, multi-writer
optimistic concurrency, and creating a brand-new table from nothing are
all implemented, tested, and **live-verified against a real MinIO
cluster**, not just a local-disk fixture. Every language with an Arrow
Flight client binding — Python, Java, Go, Rust, JS — talks to it
unmodified. `labukdb-core` has no Flight/gRPC dependency, so it embeds
like DuckDB or runs as a server like Postgres.

**What makes this worth looking at**, beyond "another Delta reader":
real multi-writer correctness research, with the failures kept visible
rather than papered over. A live stress test found this project's own
remote commit path *silently loses writes* under genuine concurrency on
a real (non-compliant) S3-compatible deployment — root-caused precisely,
fixed with an opt-in external lock, and proven fixed against the same
live cluster. A second stress test found Iceberg's own upstream
`HadoopTables` writer has the *identical class* of bug, also root-caused
down to the exact library code and the exact storage-layer assumption
that doesn't hold on object storage. Both findings — and the fixes — are
documented in detail, not hidden: see [Status at a glance](#status-at-a-glance).

Companion project to `local-lakehouse-lab` (not part of it): that project
is a Spark/Flink + DuckDB stack. This one is the from-scratch story —
reading and writing the same `_delta_log` + Parquet format with its own
log parsing, pruning, encode/decode, and write path, to understand the
mechanics end to end rather than delegate to an existing engine. Not a
full SQL engine (`GROUP BY`/`ORDER BY`/`LIMIT` and a narrow SQL subset via
CLI `--sql` are real; no joins or subqueries) and not a Spark replacement
— different scope, even though it now reads/writes real tables Spark can
also read.

For the full guided tour, see **[ARCHITECTURE.md](ARCHITECTURE.md)** (the
original read-path walkthrough) and
**[docs/labukdb-v2-architecture.md](docs/labukdb-v2-architecture.md)**
(current module design, every write-path decision, and the full
root-cause writeups referenced below). For every CLI flag, SQL subset,
and Flight wire-format field, see **[USAGE.md](USAGE.md)**.

---

## Quick start

```
mvn clean package          # runs tests too; add -DskipTests to skip them
```

Produces `labukdb-cli/target/labukdb-cli.jar` (one file, every dependency
bundled) and `labukdb-server-app/target/labukdb-server-app.jar` (the
standalone Flight server).

```
bin/labukdb catalog add logs D:\repo\local-lakehouse-lab\lakehouse\logs_delta
bin/labukdb catalog list
bin/labukdb describe logs
bin/labukdb query --table logs --filter "level=ERROR" --columns service,message --limit 10
```

(Windows: `bin\labukdb.bat ...`. Zip extraction on Windows won't preserve
the executable bit on `bin/labukdb` — `chmod +x bin/labukdb` once in Git
Bash, or just use `java -jar labukdb-cli/target/labukdb-cli.jar ...`.)

Every subcommand accepts either a catalog name or a raw filesystem/URI
path wherever a table is expected. Filter syntax: `=`, `!=`, `<`, `<=`,
`>`, `>=`, `IN`, `LIKE` (SQL-style, `%`/`_`), `AND`/`OR` — no generic
`NOT`, see `Predicate`'s javadoc for why that doesn't lose expressiveness.

**Running the Flight server:**

```
java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED \
     -jar labukdb-server-app/target/labukdb-server-app.jar
```

(The `--add-opens` flag is needed because Arrow's off-heap memory library
requires it on Java 9+; `mvn test` sets this automatically via Surefire's
`argLine` — running the built jar directly needs it passed explicitly.)

---

## Architecture at a glance

```
  Read:  Delta/Iceberg table → 1. log/ (resolve) → 2. planner/ (prune) → 3. decode/ (Parquet → Arrow) ─┐
  Write: Delta table ← log/ (commit: append/MERGE/compact/vacuum/create) ← encode/ (Arrow → Parquet) ──┤
                                                                                                          │
                                              4. flight/  (DoGet / DoPut)  ←───────────────────────────┘
                                                        │
                        Python / Java / Go / Rust / JS / LLM / dashboards / MCP
```

| Area | Package | Job | Key types |
|---|---|---|---|
| Read | `log/` | Parse the commit log, resolve a snapshot at a version, timestamp, or `latest()`. Incrementally cached; falls back to checkpoints where available. | `CommitLogReader`, `SnapshotManager`, `CheckpointReader` |
| Read | `planner/` | Decide which files can be skipped, without opening them, using partition values and file-level stats. Fail-open: anything it can't decide, it keeps. | `Predicate`, `PartitionPruner`, `FileStatsPruner`, `QueryPlanner` |
| Read | `decode/` | Per surviving file: prune row groups via Parquet footer stats, decode only requested columns, apply the residual filter row-by-row, reconstruct partition columns as constants. Decode and filter are fused into one pass. | `RowGroupPruner`, `GroupPredicateEvaluator`, `ParquetToArrowReader` |
| Write | `encode/` | Arrow → Parquet (the write mirror of `decode/`), and Arrow `Schema` → Delta's JSON `schemaString`. | `ArrowToParquetWriter`, `ArrowToDeltaSchema` |
| Write | `log/` | Append, upsert/MERGE by key, compact, vacuum, create a table — atomic commits with multi-writer optimistic concurrency control. | `CommitWriter`, `WriteBuffer`, `TableMerger`, `Compactor`, `Vacuum`, `TableCreator`, `LockProvider` |
| Both | `flight/` | Serve it. A plain scan returns one `FlightEndpoint` per file; a query with `groupBy`/`aggregates`/`orderBy`/`limit` returns one combined endpoint, decoded and run through `execution/` server-side. `DoPut` buffers and flushes writes. | `LabukDbFlightProducer`, `LabukDbServerApp` |
| — | `execution/` | `GROUP BY` aggregation and `ORDER BY`/`LIMIT`, reachable via Flight's `DoGet` and usable standalone as a library. | `AggregateOperator`, `SortLimitOperator` |

Supporting packages: `storage/` (local/S3/MinIO/ADLS location resolution,
`LockProvider` configuration), `catalog/` (`TableCatalog` maps a short
name to a table location, persisted to `~/.labukdb/catalog.json` — not
Unity Catalog, doesn't try to be), `cli/` (the `labukdb` tool), `format/`
(the `TableFormat` SPI Delta and
Iceberg both implement, auto-detected by probing for `_delta_log/` vs.
`metadata/`), `query/` (SQL text → the engine's own query structures, via
`SqlQueryTranslator`).

**Multi-module:** `labukdb-core` (everything above except `flight/`,
embeddable like DuckDB), `labukdb-flight` (Flight serving),
`labukdb-format-iceberg` (Iceberg support), `labukdb-server-app` (the
runnable server jar), `labukdb-cli` (the `labukdb` binary). See
[docs/labukdb-v2-architecture.md](docs/labukdb-v2-architecture.md) for
the full module design and every write-path decision.

---

## Usage

Four ways to ask the same engine a question — see
**[USAGE.md](USAGE.md)** for the full reference (every CLI flag, every
`FlightQueryRequest`/`FlightWriteRequest` field, the embedded library's
write path).

**CLI, flag-based:**
```
bin/labukdb query --table logs --filter "level=ERROR" --columns service,message --limit 10
```

**CLI, SQL** — a real `SELECT`, parsed into the engine's own executed
structures (`SELECT <col>|<AGGFUNC>(<col>) AS <alias> FROM <table> [WHERE ...] [GROUP BY ...] [ORDER BY ...] [LIMIT n]`;
no `NOT`/joins/subqueries/`COUNT(*)`, rejected with a specific error, not
silently mishandled):
```
bin/labukdb query --sql "SELECT level, COUNT(amount) AS cnt FROM logs GROUP BY level"
```

**Flight, JSON request** — against a running server, JSON-encoded as a
`FlightDescriptor`'s command bytes:
```json
{"table": "logs", "groupBy": ["level"],
 "aggregates": [{"function": "COUNT", "column": "amount", "outputName": "cnt"}],
 "orderBy": [{"column": "cnt", "ascending": false}], "limit": 10}
```
No SQL-over-Flight yet — a client builds this JSON shape directly.

**Embedded library, direct API** — `labukdb-core` in-process, no CLI, no
server. Not published to Maven Central — `mvn clean install -DskipTests`
first, then depend on `com.labuk:labukdb-core:0.1.0-SNAPSHOT` like any
other artifact:

```java
SnapshotManager snapshotManager = new SnapshotManager(tableRoot);
Snapshot snapshot = snapshotManager.latest();

Predicate filter = Predicate.compare("level", ComparisonOp.EQ, "ERROR");
ScanPlan plan = new QueryPlanner().plan(snapshot, filter, List.of("service", "message"));

ParquetToArrowReader reader = new ParquetToArrowReader();
try (RootAllocator allocator = new RootAllocator()) {
    for (AddFile file : plan.getFilesToScan()) {
        Predicate residual = PartitionValueSubstituter.simplify(
                plan.getResidualPredicate(), file.getPartitionValues());
        try (VectorSchemaRoot root = reader.read(tableRoot.resolve(file.getPath()).toFile(),
                allocator, plan.getProjectedColumns(), residual, file.getPartitionValues())) {
            System.out.println(root.contentToTSVString());
        }
    }
}
```

Writing — `TableCreator`/`CommitWriter` both take a `TableLocation`, so
this works identically against a local path or `s3a://`/`abfss://`:

```java
TableLocation location = TableLocation.forLocal(Path.of("/path/to/new/table"));
new TableCreator().createTable(location, schema, List.of("level"), null);
CommitWriter.forLocation(location).appendBatch(batch, Map.of("level", "ERROR"));
```

`TableMerger.merge(...)` (upsert/delete by key), `Compactor`, and
`Vacuum` all have matching `TableLocation` overloads, local or remote.
For genuinely concurrent remote writers, see the OCC row in
[Status at a glance](#status-at-a-glance) before relying on this against
object storage.

---

## Status at a glance

| Area | Status | Notes |
|---|---|---|
| Delta read (local, MinIO) | ✅ Done, live-verified | Remote fully `TableLocation`-dispatched |
| Delta write — append/MERGE/compact/vacuum/create (local, MinIO) | ✅ Done, live-verified | Including `Vacuum`'s real remote delete, confirmed via a live `FileSystem.exists()` check |
| Delta multi-writer OCC, remote, no lock | 🔴 **Confirmed broken** on at least one real MinIO deployment | Silent lost updates under genuine concurrency — root-caused in `docs/labukdb-v2-architecture.md`'s "Concurrent writes" section. Constructing this configuration now logs a proportionate warning (`RemoteWriteSafetyWarning`) |
| Delta multi-writer OCC, remote, with lock | ✅ Done, live-verified | Opt-in `LockProvider`/`DynamoDbLockProvider`; same failing scenario passes with it engaged. Scoped as a mitigation for non-compliant S3-compatible storage, not a universal requirement |
| Delta CLI `--sql` (local, MinIO) | ✅ Done, live-verified | `GROUP BY`, `ORDER BY`/`LIMIT`, `WHERE` |
| Iceberg read + REST catalog (local, MinIO) | ✅ Done, live-verified | REST catalog proven against a real `tabulario/iceberg-rest` server |
| Iceberg write — append, no lock | 🔴 **Confirmed broken** under genuine concurrent writers on real MinIO | Single-writer safe. Root cause traced into `iceberg-core`'s own `HadoopTableOperations`, same class of bug as the Delta finding above. Also now warns on construction (`RemoteWriteSafetyWarning`) |
| Iceberg write — append, with lock | ✅ Done, live-verified | Opt-in `DynamoDbLockManager` via Iceberg's own `LockManager` extension point; same failing scenario passes with it engaged |
| Iceberg CREATE TABLE | ✅ Done | `IcebergTableCreator`, path-based and catalog-based; identity-only partitioning |
| Iceberg MERGE/upsert | ✅ Done | `IcebergTableMerger`, path-based and catalog-based; reuses Delta's exact merge semantics via shared `PartitionRowMerger` |
| Iceberg compaction / vacuum | ❌ Not built | |
| Type support | 🟡 Most primitives, DECIMAL (Decimal128), `LIST<float/double/int/long/string>` (read + write) | Not yet: nested `LIST<LIST<...>>`, MAP, STRUCT |
| Checkpoints | 🟡 Parsing tested against a self-built fixture | Not yet proven against a real Spark-produced checkpoint — highest-risk, least-verified code in the project |
| Real AWS S3 / Azure ADLS (not MinIO) | 🟡 Implemented, not independently verified | MinIO is the only remote backend actually stress-tested live |
| Flight surface | 🟡 `GetFlightInfo`/`DoGet`/`DoPut`/`ListFlights` done | `DoAction`/`DoExchange` unimplemented — no CREATE TABLE RPC, no push-based tailing |
| Caching | ✅ Snapshot reuse (`TableCache`), Parquet schema caching (`ParquetFooterCache`) | A decoded-row-batch LRU for hot files is not yet implemented |

Three findings below have full root-cause writeups in
[docs/labukdb-v2-architecture.md](docs/labukdb-v2-architecture.md) rather
than duplicated here — each is the kind of thing worth reading in full:
**the Delta remote-OCC lock story** (what actually fails on non-compliant
S3-compatible storage, and why the fix is a DynamoDB-backed lock, not a
smarter retry), **the Iceberg `HadoopTables` root cause** (traced through
real `iceberg-core` source to the exact line where the atomicity
assumption breaks), and **a known, pre-existing `FlightIntegrationTest`
flake** (confirmed *not* the Arrow issue once blamed for it — the real
cause is still open).

---

## Storage backends

Local disk, S3/MinIO (`s3a://`), and ADLS Gen2 (`abfss://`) all resolve
through one entry point, `TableLocation`, by URI scheme.
`hadoop-aws`/`hadoop-azure` are `optional` in `labukdb-core`'s own pom —
embedding just `labukdb-core` doesn't pull in either cloud SDK. The
shipped `labukdb-server-app.jar`/`labukdb-cli.jar` declare them
non-optional (so the advertised `s3a://`/`abfss://` support actually
ships) — real cost: both jars are ~780MB, dominated by the AWS SDK's
all-services bundle. An open, undecided trade-off.

### Configuring a remote backend

No config file — the CLI and Flight server read connector settings from
environment variables automatically whenever a table location uses a
remote URI scheme:

| Backend | Environment variables | Example location |
|---|---|---|
| Local disk | *(none)* | `/path/to/table` |
| MinIO | `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` | `s3a://bucket/table` |
| S3 (real AWS) | *(none specific)* — falls back to Hadoop's default AWS credential chain when no `MINIO_*` vars are set | `s3a://bucket/table` |
| ADLS Gen2 | `ADLS_STORAGE_ACCOUNT`, `ADLS_AUTH_TYPE` (`SharedKey`/`OAuth`), then the matching key/OAuth vars | `abfss://container@account.dfs.core.windows.net/table` |
| Iceberg REST catalog | `ICEBERG_REST_URI` (required), plus warehouse/auth vars — see [USAGE.md](USAGE.md#iceberg-via-a-rest-catalog) | `icebergrest://namespace.table` |
| Remote OCC lock (Delta, opt-in) | `DYNAMODB_LOCK_TABLE` (required to enable), `DYNAMODB_REGION`, `DYNAMODB_ENDPOINT` (for `amazon/dynamodb-local`), `DYNAMODB_ACCESS_KEY`/`_SECRET_KEY` | — |

```
export MINIO_ENDPOINT=https://your-minio-host
export MINIO_ACCESS_KEY=...
export MINIO_SECRET_KEY=...
bin/labukdb catalog add logs s3a://bucket/path/to/table
```

Embedding `labukdb-core` directly: no env-var auto-detection at that
layer — build the `Configuration` yourself:

```java
Configuration conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
TableLocation location = TableLocation.forRemote("s3a://bucket/path", conf);
```

---

## Python client & RAG demo

- `python/query_demo.py` — a plain `pyarrow.flight` client against a
  running `labukdb-server-app`. Proof any Flight-capable client in any
  language works against this server unmodified.
- `python/rag_demo.py` — a small grounded-QA demo using two local Ollama
  calls: one turns a plain-English question into the JSON query
  `query_demo.py` sends by hand, the other answers using the actual
  retrieved rows. Fully local, no paid API.

---

## Built on

| Library | Used for |
|---|---|
| [Apache Parquet](https://parquet.apache.org/) | Footer statistics, row-group reads, the `Group` record API |
| [Apache Arrow](https://arrow.apache.org/) | The in-memory columnar format every decoded row lands in |
| [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) | The gRPC-based wire protocol `flight/` serves over |
| [Apache Hadoop](https://hadoop.apache.org/) (`FileSystem` API only) | Storage abstraction — no MapReduce/YARN/HDFS daemons, see [HADOOP_USAGE.md](HADOOP_USAGE.md) |
| [Jackson](https://github.com/FasterXML/jackson) | Parsing/writing `_delta_log` JSON commits |

## Inspired by, deliberately not depending on

The point is understanding the mechanics first-hand, so these were read
as references, not pulled in as dependencies:

- **[Delta Lake transaction log protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)** —
  the spec `log/`/`planner/` implement against.
- **[delta-kernel-rs](https://github.com/delta-io/delta-kernel-rs)** —
  Delta's own "build an engine on top of this" library, deliberately not
  used — the log replay, pruning, and decode it would hand you are
  implemented from scratch here instead.
- **[delta-rs](https://github.com/delta-io/delta-rs)** — another
  from-scratch (Rust) implementation, a sanity check for "does my
  understanding of the protocol match an independent one."
- **[DuckDB](https://duckdb.org/)** — the natural "just use this
  instead" answer this project deliberately isn't taking.

---

## Roadmap

Full backlog (every item, every status) lives in
[docs/labukdb-v2-architecture.md](docs/labukdb-v2-architecture.md)'s
"Build order" and "Phase roadmap" sections — this is the short version.

| Area | Next up |
|---|---|
| Write path | Iceberg compaction/vacuum (CREATE TABLE + MERGE done); CLI write subcommands (`ingest`/`compact`/`vacuum`/`create-table`); Flight `DoAction` for CREATE TABLE; orphan-file detection in `Vacuum`; read-path proactive recovery for the remote OCC lock |
| Core engine | Nested `LIST<LIST<...>>`/MAP/STRUCT; checkpoint writing (reads only today); row-data LRU cache; SQL-over-Flight; `DoExchange` streaming/tailing |
| Multi-format | Hive Metastore/Glue/Nessie catalogs; Parquet-only (no-log) tables |
| AI / LLM | MCP tool wrapper; catalog semantic descriptions; case-insensitive filters |
| Observability | Grafana as a Flight client; Prometheus metrics; InfluxDB → Delta pipeline |
| Infrastructure | Distributed Flight/read scale-out; distributed ingestion fleet (planned, not designed — the largest remaining piece); distributed query execution (planned, not designed); auth; Docker/Helm |

---

*Copyright 2026 Afzal and lakehouse-flight contributors.
Licensed under the Apache License, Version 2.0 — see [LICENSE](LICENSE).*
