# labukDB

[![CI](https://github.com/codelabuk/lakehouse-flight/actions/workflows/ci.yml/badge.svg)](https://github.com/codelabuk/lakehouse-flight/actions)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

**A from-scratch Delta Lake database — reads and writes, local or remote,
embeddable or served over Apache Arrow Flight.**

It reads and writes real Delta tables — local disk, S3, MinIO, or ADLS —
prunes files down using a four-level pushdown pipeline on read, and
streams rows in or out as Arrow IPC to any client in any language. It also
reads (not yet writes) Iceberg tables. No `delta-rs`, no `delta-kernel`,
no DuckDB/Spark in the path: the transaction log, pruning, Parquet
encode/decode, and the write path (append, upsert/MERGE by key,
compaction, vacuum, multi-writer concurrency, table creation) are all
implemented here, one small and independently testable layer at a time.

For a full guided tour of how it works, see **[ARCHITECTURE.md](ARCHITECTURE.md)**
(the original read-path walkthrough) and
**[docs/labukdb-v2-architecture.md](docs/labukdb-v2-architecture.md)**
(the current module design, including everything the write path added).
For every way to actually *use* it — CLI flags, CLI SQL, the Flight JSON
wire format, or embedding the library directly — with every field and
flag documented, see **[USAGE.md](USAGE.md)**. This README covers what
the project is, how the pieces fit together at a glance, and how to
build and run it.

---

## What this is

- **A real database, not just a reader.** Append, upsert/MERGE by key,
  compaction, vacuum, multi-writer optimistic concurrency, and creating a
  brand-new table from nothing are all implemented and tested — this
  stopped being read-only a while ago.
- **Built from `_delta_log` up.** Every layer (log parsing, pruning,
  Parquet encode/decode, the write path, Flight serving) is its own
  package, readable and tested on its own.
- **A real Flight server.** Any language with an Arrow Flight client
  binding — Python, Java, Go, Rust, JS — talks to it unmodified, for both
  reads (`DoGet`) and writes (`DoPut`).
- **Embeddable too.** `labukdb-core` has no Flight/gRPC dependency — use
  it as a plain library in your own JVM process, DuckDB-style, or run the
  Flight server for network access, Postgres-style.
- **Honest about its gaps.** See [Status & known limitations](#status--known-limitations)
  below rather than a marketing pass.

## What this is not

- **Not a SQL engine.** `GROUP BY`/`ORDER BY`/`LIMIT` are real, served
  over `DoGet` — see [The layers](#the-layers) — but there's no SQL text
  parser; a client still builds a request shape, not a SQL string. No
  joins.
- **Not a Spark replacement** — different scope entirely, though it now
  overlaps more than it used to (it writes real Delta tables Spark can
  also read).
- **Not Iceberg-write-capable yet** — Iceberg support is read-only.

Companion project to `local-lakehouse-lab` (not part of it): that project
is a Spark/Flink + DuckDB stack. This one is the from-scratch story —
reading and writing the same `_delta_log` + Parquet format with its own
log parsing, pruning, encode/decode, and write path instead of delegating
to an existing engine.

## Why build this instead of using delta-kernel / DuckDB

The goal is understanding the mechanics, not shipping fastest. Each layer
below is deliberately small enough to reason about end to end.

---

## The layers

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
| Read | `planner/` | Decide which files can be skipped, without opening them, using partition values and file-level stats from the log. Fail-open: anything it can't decide, it keeps. | `Predicate`, `PartitionPruner`, `FileStatsPruner`, `QueryPlanner` |
| Read | `decode/` | For each surviving file: prune row groups via Parquet footer stats, decode only the requested columns, apply the residual filter row-by-row, and reconstruct partition columns as constants. Decode and filter are fused into one pass. | `RowGroupPruner`, `GroupPredicateEvaluator`, `ParquetToArrowReader` |
| Write | `encode/` | Arrow → Parquet (the write mirror of `decode/`), and Arrow `Schema` → Delta's JSON `schemaString` for creating a table. | `ArrowToParquetWriter`, `ArrowToDeltaSchema` |
| Write | `log/` | Append a batch, upsert/MERGE by key, compact small files, vacuum expired ones, create a brand-new table — all as atomic commits with multi-writer optimistic concurrency control. | `CommitWriter`, `WriteBuffer`, `TableMerger`, `Compactor`, `Vacuum`, `TableCreator` |
| Both | `flight/` | Serve it. A plain scan returns one `FlightEndpoint` per surviving file, decoded lazily per ticket. A query with `groupBy`/`aggregates`/`orderBy`/`limit` returns one combined endpoint instead — every surviving file is decoded, combined, and run through `execution/` server-side before streaming back one correct, cross-file result. `DoPut` for writes — streams incoming batches into a buffer, flushes to a committed file. | `DeltaFlightProducer`, `LabukDbServerApp` |
| — | `execution/` | `GROUP BY` aggregation and `ORDER BY`/`LIMIT`, reachable via `flight/`'s `DoGet` (see above) and usable standalone as a library. | `AggregateOperator`, `SortLimitOperator` |

Supporting packages: `storage/` (local/S3/MinIO/ADLS location resolution),
`catalog/` (name → table-root registry), `cli/` (the `labukdb` tool),
`format/` (the `TableFormat` SPI Delta and Iceberg both implement —
`DetectedFormat` auto-detects which one a location holds by probing for
`_delta_log/` vs. `metadata/`, and `cli`/`flight` dispatch through it for
Iceberg tables specifically; Delta's own query path still goes straight
through `log`/`planner` unchanged, see [Query/analytics support](#status--known-limitations)
below for why), `query/` (SQL text → the engine's own `Predicate`/
`AggregateSpec`/`SortKey` structures, via `SqlQueryTranslator`).

**Multi-module:** `labukdb-core` (everything above except `flight/`, no
Flight/gRPC dependency, embeddable like DuckDB), `labukdb-flight` (Flight
serving), `labukdb-format-iceberg` (read-only Iceberg support — optional
for `labukdb-core` embedders, but a required dependency of `labukdb-cli`
and `labukdb-flight` themselves now, since both query Iceberg tables
directly), `labukdb-server-app` (the runnable server jar), `labukdb-cli` (the
`labukdb` binary — `catalog`/`describe`/`query` only today, no write
subcommands yet). See
[docs/labukdb-v2-architecture.md](docs/labukdb-v2-architecture.md) for
the full module design, every write-path decision, and what's still
planned on top of it.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full walkthrough of each
layer, including why a table is a log and not a folder, how pruning stays
correct under ambiguity, and what "zero-copy" actually means here.

---

## Building and running

```
mvn clean package          # runs tests too; add -DskipTests to skip them
```

This produces `labukdb-cli/target/labukdb-cli.jar` — one file, every
dependency bundled in — and `labukdb-server-app/target/labukdb-server-app.jar`,
the standalone Flight server.

```
bin/labukdb catalog add logs D:\repo\local-lakehouse-lab\lakehouse\logs_delta
bin/labukdb catalog list
bin/labukdb describe logs
bin/labukdb query --table logs --filter "level=ERROR" --columns service,message --limit 10
```

(Windows: `bin\labukdb.bat ...`. Zip extraction on Windows won't preserve
the executable bit on `bin/labukdb` — run `chmod +x bin/labukdb` once
in Git Bash, or just use `java -jar labukdb-cli/target/labukdb-cli.jar ...`
directly.)

Every subcommand accepts either a catalog name or a raw filesystem/URI path
wherever a table is expected.

### Running the Flight server

```
java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED \
     -jar labukdb-server-app/target/labukdb-server-app.jar
```

The `--add-opens` flag is needed because Arrow's off-heap memory library
requires it on Java 9+. `mvn test` sets this automatically via Surefire's
`argLine`; running the built jar directly needs it passed explicitly.

### Filter syntax

`=`, `!=`, `<`, `<=`, `>`, `>=`, `IN`, `LIKE` (SQL-style, `%`/`_`,
case-sensitive), combined with `AND`/`OR`. No generic `NOT` — see
`Predicate`'s javadoc for why that doesn't lose expressiveness.

---

## Ways to query labukDB

See **[USAGE.md](USAGE.md)** for the full reference (every CLI flag,
every `FlightQueryRequest`/`FlightWriteRequest` field, and the embedded
library's write path too) — this section is a quick summary.

Four ways to ask the same engine a question, from least to most setup:

**1. CLI, flag-based** — `--filter` is deliberately simple (`AND`-only,
see [Filter syntax](#filter-syntax) above):

```
bin/labukdb query --table logs --filter "level=ERROR" --columns service,message --limit 10
```

**2. CLI, SQL** — a real `SELECT` statement, parsed by `SqlQueryTranslator`
into the exact `Predicate`/`AggregateSpec`/`SortKey` structures the engine
already executes correctly. Mutually exclusive with
`--table`/`--columns`/`--filter`/`--limit` — the SQL text's own `FROM`/
`SELECT`/`WHERE`/`LIMIT` clauses subsume all four:

```
bin/labukdb query --sql "SELECT level, COUNT(amount) AS cnt FROM logs GROUP BY level"
bin/labukdb query --sql "SELECT amount FROM logs ORDER BY amount DESC LIMIT 3"
```

Subset supported: `SELECT <col>|<AGGFUNC>(<col>) AS <alias> FROM <table>
[WHERE <predicate>] [GROUP BY ...] [ORDER BY ...] [LIMIT n]`, with the
same predicate operators and aggregate functions as above. Rejected with
a specific error, not silently mishandled: `NOT`/`NOT IN`/`NOT LIKE`,
`IS NULL`, joins, subqueries, `COUNT(*)` (matches `AggregateOperator`'s
pre-existing `COUNT(column)`-only semantics) — and, confirmed against a
real parse, `IN` combined with `AND`/`OR` without explicit parentheses
(a genuine `jsqlparser` grammar precedence quirk where a bare `IN (...)`
greedily consumes a trailing `OR`/`AND`; wrap it, e.g.
`(level IN ('a','b')) OR (x LIKE 'c')`). See `SqlQueryTranslator`'s
javadoc for the full, exact subset. A plain `SELECT`+`WHERE` reuses the
same per-file streaming scan as `--filter`; anything needing `GROUP BY`/
`ORDER BY`/`LIMIT` goes through the same cross-file `CombinedQueryExecutor`
the Flight server uses below — one real implementation, not two that
could drift.

**3. Flight, JSON request** — a running server, queried over gRPC. The
request is JSON-encoded as a `FlightDescriptor`'s command bytes:

```json
{
  "table": "logs",
  "groupBy": ["level"],
  "aggregates": [
    {"function": "COUNT", "column": "amount", "outputName": "cnt"}
  ],
  "orderBy": [{"column": "cnt", "ascending": false}],
  "limit": 10
}
```

Any of `groupBy`/`aggregates`/`orderBy`/`limit` switches `GetFlightInfo`
onto a single combined endpoint instead of one-per-file — see
`FlightQueryRequest`'s javadoc for the complete field list (predicate
shape, `version`/`timestampMillis` for time travel, etc). **No SQL-over-
Flight yet** — a client builds this JSON shape directly today; translating
SQL text into it server-side is a deliberate, separate follow-up, not
built in this pass (CLI-only).

**4. Embedded library, direct API** — no CLI, no server, just
`labukdb-core` in-process; see [Using it as a library](#using-it-as-a-library)
below for the full `log/` → `planner/` → `decode/` walkthrough.

---

## Using it as a library

There are three ways to use this engine — the `labukdb` CLI binary, a
running Flight server queried remotely, and calling the Java API directly
in-process (what both of the above actually do under the hood). The third
option is useful if you're embedding a Delta reader inside your own JVM
application rather than shelling out or opening a Flight connection —
`labukdb-core` has no Flight/gRPC dependency, so this is the DuckDB-style
embedded path.

Not published to Maven Central — build and install it into your local
`~/.m2` repository first:

```
mvn clean install -DskipTests
```

Then depend on just the core module, like any other Maven artifact:

```xml
<dependency>
    <groupId>com.labuk</groupId>
    <artifactId>labukdb-core</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

The four layers map directly onto four calls — resolve a snapshot, plan
which files survive pruning, fold each file's partition values into the
residual predicate, then decode:

```java
import com.labuk.labukdb.core.log.Snapshot;
import com.labuk.labukdb.core.log.SnapshotManager;
import com.labuk.labukdb.core.log.action.AddFile;
import com.labuk.labukdb.core.planner.*;
import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.File;
import java.nio.file.Path;
import java.util.List;

Path tableRoot = Path.of("D:/repo/local-lakehouse-lab/lakehouse/logs_delta");

// 1. log/ — resolve the current (or a historical) snapshot
SnapshotManager snapshotManager = new SnapshotManager(tableRoot);
Snapshot snapshot = snapshotManager.latest();          // or atVersion(n) / atTimestamp(instant)

// 2. planner/ — decide which files survive pruning
Predicate filter = Predicate.compare("level", ComparisonOp.EQ, "ERROR");
QueryPlanner planner = new QueryPlanner();
ScanPlan plan = planner.plan(snapshot, filter, List.of("service", "message"));

System.out.println(plan.getFilesToScan().size() + " of "
        + plan.getTotalFilesInSnapshot() + " files survived pruning");

// 3. decode/ — read each surviving file (partition columns folded to constants)
ParquetToArrowReader reader = new ParquetToArrowReader();
try (RootAllocator allocator = new RootAllocator()) {
    for (AddFile file : plan.getFilesToScan()) {
        Predicate residual = PartitionValueSubstituter.simplify(
                plan.getResidualPredicate(), file.getPartitionValues());
        File parquetFile = tableRoot.resolve(file.getPath()).toFile();

        try (VectorSchemaRoot root = reader.read(
                parquetFile, allocator, plan.getProjectedColumns(), residual, file.getPartitionValues())) {
            System.out.println(root.contentToTSVString());   // one Arrow batch per file
        }
    }
}
```

`SnapshotManager` also has a `TableLocation`-aware constructor for S3/MinIO/
ADLS tables (`SnapshotManager.forLocation(...)`), and `ParquetToArrowReader`
has matching `TableLocation`-based `readSchema`/`read` overloads — see
`labukdb-core/src/main/java/com/labuk/labukdb/core/tools/RealTableSmokeCheck.java`
for a second, runnable end-to-end example (schema inspection plus a
side-by-side pruned vs. unpruned file count), and `DeltaFlightProducer`
(in `labukdb-flight`) for exactly this same sequence wired into
`GetFlightInfo`/`DoGet`.

### Writing

Creating a table, then appending to it — `TableCreator` and
`CommitWriter` both take a `TableLocation`, so this works identically
against a local path or an `s3a://`/`abfss://` URI (given the right
`Configuration` — see [Configuring a remote backend](#configuring-a-remote-backend)):

```java
import com.labuk.labukdb.core.log.CommitWriter;
import com.labuk.labukdb.core.log.TableCreator;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.vector.types.pojo.*;

TableLocation location = TableLocation.forLocal(Path.of("/path/to/new/table"));
Field service = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
Field amount = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);

new TableCreator().createTable(location, new Schema(List.of(service, amount)),
        List.of("level"), null);   // partitioned by "level", no table properties

// ... build a VectorSchemaRoot batch, then:
CommitWriter.forLocation(location).appendBatch(batch, Map.of("level", "ERROR"));
```

`TableMerger.merge(...)` does insert/update/delete by key on top of this,
local or remote (`merge(TableLocation, ...)`, same `forLocation`-style
pattern) — see its javadoc for the exact semantics (three separate
batches, not an operation-indicator column; key supplied per call, not
stored table metadata). `Compactor`/`Vacuum` have matching
`TableLocation` overloads too.

---

## The catalog

`TableCatalog` maps a short name to a table location, persisted to
`~/.labukdb/catalog.json` — so a client (especially an AI agent or MCP
tool) can say `"logs"` instead of needing to know an exact path. Not
Unity Catalog, and doesn't try to be.

## Storage backends

Local disk, S3/MinIO (`s3a://`), and ADLS Gen2 (`abfss://`) are all
resolved through one entry point, `TableLocation`, by URI scheme.
`hadoop-aws`/`hadoop-azure` are `optional` in `labukdb-core`'s own pom —
a library consumer embedding just `labukdb-core` isn't forced to pull in
either cloud SDK. The **shipped** `labukdb-server-app.jar`/`labukdb-cli.jar`
are different: both declare these non-optional, since they're the actual
runnable binaries the README advertises as able to read/write `s3a://`/
`abfss://` tables — without that override, the shaded jars would silently
ship with zero S3A/ADLS classes. Real, measured cost of that: both jars
are ~780MB, dominated by AWS SDK v2's all-services "bundle" (not just
S3 — `hadoop-aws` pulls in every AWS service client). An open, undecided
trade-off — accept the size, split into lean/cloud build variants, or
something else.

**Read path:** fully remote-capable — log/metadata and the Parquet data
read in `decode/` both dispatch through `TableLocation`, the same path
`DeltaFlightProducer` uses for every query. Live-verified against a real
MinIO cluster (scan, partition pruning, decode).

**Write path:** also fully remote-capable now — `CommitWriter`,
`ArrowToParquetWriter`, `WriteBuffer`, and `TableCreator` all dispatch
through `TableLocation`, local or remote, the same pattern the read path
uses. Live-verified against a real MinIO cluster end to end: creating a
brand-new table, appending data to it, and reading it back — both as a
library call and through a real `DoPut`/`DoGet` round trip over Flight.
`Compactor`, `Vacuum`, and `TableMerger` (upsert/MERGE) are also fully
remote-capable now, live-MinIO-verified — the same additive
`TableLocation`-overload pattern, with one new piece `Vacuum` needed on
top of it: an actual remote file **delete** (Hadoop `FileSystem.delete`),
confirmed with a real `FileSystem.exists()` check proving bytes are
genuinely gone from the bucket, not just logically removed from the log.

### Configuring a remote backend

No config file today — the CLI and Flight server both read connector
settings from environment variables automatically whenever a table
location uses a remote URI scheme (`TableResolver` and
`DeltaFlightProducer`, respectively):

| Backend | Environment variables | Example location |
|---|---|---|
| Local disk | *(none)* | `/path/to/table` or `D:\path\to\table` |
| MinIO | `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` | `s3a://bucket/table` |
| S3 (real AWS) | *(none specific to this project)* — falls back to Hadoop's default AWS credential chain (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, `~/.aws/credentials`, or an IAM role) when no `MINIO_*` vars are set | `s3a://bucket/table` |
| ADLS Gen2 | `ADLS_STORAGE_ACCOUNT`, `ADLS_AUTH_TYPE` (`SharedKey` or `OAuth`), then `ADLS_ACCOUNT_KEY` **or** `ADLS_TENANT_ID`/`ADLS_CLIENT_ID`/`ADLS_CLIENT_SECRET` | `abfss://container@account.dfs.core.windows.net/table` |

```
export MINIO_ENDPOINT=https://your-minio-host
export MINIO_ACCESS_KEY=...
export MINIO_SECRET_KEY=...
bin/labukdb catalog add logs s3a://bucket/path/to/table
bin/labukdb describe logs
```

**Embedding `labukdb-core` as a library:** there's no env-var
auto-detection at that layer — build the `Configuration` yourself and
pass it explicitly, since the core engine has no opinion on where config
should come from:

```java
Configuration conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
// or AdlsConfiguration.forSharedKey(account, key) / .forServicePrincipal(...)
TableLocation location = TableLocation.forRemote("s3a://bucket/path", conf);
```

`MinioConfiguration.fromEnvironment()` / `AdlsConfiguration.fromEnvironment()`
are available to call directly too, if you want the same env-var
convenience the CLI uses inside your own application.

## Python client & RAG demo

- `python/query_demo.py` — a plain `pyarrow.flight` client against a
  running `labukdb-server-app`. Proof that any Flight-capable client in
  any language works against this server unmodified.
- `python/rag_demo.py` — a small grounded-QA demo using two local Ollama
  calls: one turns a plain-English question into the JSON query
  `query_demo.py` sends by hand (using the table's live schema, fetched
  via `GetFlightInfo`), the other answers using the actual retrieved rows.
  That split — real data in between two model calls — is what makes it RAG
  rather than "an LLM near a database." Fully local, no paid API.

---

## Status & known limitations

**Type support (`decode/`/`encode/`):** INT32, INT64, FLOAT, DOUBLE,
BOOLEAN, BINARY/UTF-8, timestamp (INT96 legacy on read via
`Int96TimestampCodec`, a modern `TIMESTAMP` logical type on write), and
`LIST<float/double/int/long/string>` for flat vector/array columns (read
only). Not yet supported: DECIMAL, nested `LIST<LIST<...>>`, MAP, STRUCT.

**Pruning:** `LIKE` filters correctly but don't get range-based pruning at
the file/row-group level yet — correctness isn't affected, since the
residual filter still applies the pattern exactly; it just means a
`LIKE`-only predicate skips no files at those levels.

**Checkpoints:** `CheckpointReader` hand-parses Delta's nested checkpoint
Parquet schema against the protocol spec. It's tested against a
self-built fixture, which proves the parsing mechanics are correct but
**not** that the schema assumption matches a real Spark-produced
checkpoint — that's the highest-risk, least-verified code in the project.
Multi-part checkpoints aren't supported (fail loudly, not silently).

**Caching:** `SnapshotManager` reuse (`TableCache`) and Parquet schema
caching (`ParquetFooterCache`) are both in place. A decoded-row-batch LRU
for hot files is not yet implemented.

**Flight surface:** `GetFlightInfo`, `DoGet`, `DoPut`, and `ListFlights`
are implemented, local and remote — including `GROUP BY`/`ORDER BY`/
`LIMIT` over `DoGet` (see Query/analytics support below). `DoAction`/
`DoExchange` are unimplemented — there's no CREATE TABLE RPC and no
push-based tailing/subscription yet.

**Write support:**
- **Append** (`CommitWriter.appendBatch`, `WriteBuffer`/`DoPut`) — done,
  local and remote, with multi-writer optimistic concurrency control
  (bounded retry, conflict detection — real 10-thread concurrency tests).
- **Upsert/MERGE by key** (`TableMerger`) — done, local and remote,
  live-MinIO-verified. The key is supplied fresh on every call, not
  stored as table metadata — matches real Delta `MERGE INTO`'s own
  behavior rather than inventing a non-standard extension to the Delta
  protocol.
- **Compaction and vacuum** (`Compactor`, `Vacuum`) — done, local and
  remote, live-MinIO-verified (`Vacuum`'s remote delete via Hadoop
  `FileSystem.delete`, confirmed with a real `FileSystem.exists()` check
  that bytes are actually gone). No size/age heuristic for which files
  to compact yet (merges everything in a partition every time); no
  orphan-file detection in `Vacuum` (only acts on files a `remove`
  action actually named).
- **CREATE TABLE** (`TableCreator`) — done, local and remote, live-MinIO-
  verified. Library capability only — no Flight `DoAction` RPC or CLI
  subcommand yet.
- **Not implemented:** writing to Iceberg tables (read-only today); CLI
  write subcommands (`ingest`/`compact`/`vacuum`/`create-table` — the CLI
  is `catalog`/`describe`/`query` only); remote multi-writer OCC has the
  right exception-handling in place but hasn't been separately
  stress-tested under concurrent remote writers the way local disk has.

**Query/analytics support:** `GROUP BY` (`SUM`/`COUNT`/`MIN`/`MAX`/`AVG`)
and `ORDER BY`/`LIMIT` are both reachable through a real `DoGet` round
trip, not just available as a library — `GetFlightInfo` switches to a
single combined endpoint whenever any of `groupBy`/`aggregates`/
`orderBy`/`limit` is present, decodes every surviving file, combines
them server-side, and runs `execution/` against the combined result
before streaming back one correct, cross-file answer (verified with
real multi-file fixtures — a per-file-only implementation would
silently produce a wrong, partial result instead). A plain scan is
completely unaffected — same one-endpoint-per-file path as always.
`COUNT` is SQL `COUNT(column)` (non-null values), not `COUNT(*)`. A real
SQL text parser (`SqlQueryTranslator`, built on `jsqlparser`) is wired
into the CLI's `--sql` flag — see [Ways to query labukDB](#ways-to-query-labukdb)
above for the supported subset. SQL-over-Flight (translating SQL text
into the JSON request shape server-side) is not built yet; a Flight
client still builds that shape directly.

**A known, pre-existing flaky test — root cause still open, not Arrow's.**
`FlightIntegrationTest` intermittently fails (~1-in-4 observed) with
`IllegalStateException: Memory was leaked by query` during
`allocator.close()` in teardown — confirmed not tied to any specific code
path (it hits read-only tests as often as write ones, and has now also
been observed once on `FlightIcebergIntegrationTest`, a brand-new test
with no resource-handling difference from the already-stable
`FlightAggregationIntegrationTest` it mirrors — passed cleanly on every
other run, standalone and full-suite, reinforcing that this is suite-wide
timing/load-dependent flakiness, not a defect in any one test). Earlier notes in
this project cited Arrow's own
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

## Built on

Libraries this project actually depends on (see `pom.xml` for exact
versions):

| Library | Used for |
|---|---|
| [Apache Parquet (`parquet-mr`)](https://parquet.apache.org/) | Footer statistics, row-group reads, the `Group` record API `GroupPredicateEvaluator` and `CheckpointReader` decode against |
| [Apache Arrow](https://arrow.apache.org/) | The in-memory columnar format every decoded row lands in (`arrow-vector`) |
| [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) | The gRPC-based wire protocol `flight/` serves over — this is *why* any language's Flight client works unmodified |
| [Apache Hadoop](https://hadoop.apache.org/) (`FileSystem` API only) | Storage abstraction for `log/`, `decode/`, and `encode/` (read and write) — no MapReduce/YARN/HDFS daemons involved, see [HADOOP_USAGE.md](HADOOP_USAGE.md) |
| [Jackson](https://github.com/FasterXML/jackson) | Parsing and writing the `_delta_log` JSON commit files |

## Inspired by, deliberately not depending on

The point of this project is understanding the mechanics of a Delta
database first-hand, so these were read as references, not pulled in as
dependencies:

- **[Delta Lake transaction log protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)** —
  the spec `log/` and `planner/` implement against: action types
  (`add`/`remove`/`metaData`/`protocol`), the JSON commit format, and the
  checkpoint Parquet schema `CheckpointReader` assumes.
- **[delta-kernel-rs](https://github.com/delta-io/delta-kernel-rs) / delta-kernel-java** —
  Delta's own official "build an engine on top of this" library. This
  project intentionally does *not* use it — the log replay, pruning, and
  Parquet decode it would otherwise hand you are implemented from scratch
  here instead, as the whole point of the exercise.
- **[delta-rs](https://github.com/delta-io/delta-rs)** — another
  from-scratch (Rust) Delta implementation; a useful sanity check for
  "does my understanding of the protocol match another independent
  implementation's."
- **[DuckDB](https://duckdb.org/)** — what `local-lakehouse-lab`'s own
  query layer uses; the natural "just use this instead" answer this
  project is deliberately not taking, for the reasons in
  [Why build this instead of using delta-kernel / DuckDB](#why-build-this-instead-of-using-delta-kernel--duckdb).

---

## Roadmap

### Write path
| Item | Notes |
|---|---|
| Iceberg write support | `IcebergTableFormat` is read-only today; `CommitWriter`'s Delta-specific write path has no Iceberg equivalent yet |
| CLI write subcommands | `ingest`/`compact`/`vacuum`/`create-table` — the write-path classes all exist as library capabilities, none are exposed via the CLI yet |
| Flight `DoAction` for CREATE TABLE | `TableCreator` is a library capability only; needs its own request/response shape decision |
| Remote multi-writer OCC stress-testing | Conflict-retry logic handles both local (NIO) and remote (Hadoop) exception types; not separately stress-tested under concurrent remote writers the way local disk is |
| Orphan-file detection | `Vacuum` only acts on files an explicit `remove` action named — a file written but never committed (e.g. a crash mid-write) is never found, the same gap Iceberg's separate `DeleteOrphanFiles` covers |

### Core engine
| Item | Notes |
|---|---|
| Nested `LIST<LIST<...>>`, MAP, STRUCT | `LIST<primitive>` already supported, read-only |
| Checkpoint multi-part support for writes | Multi-part checkpoints are read-supported; `CommitWriter` doesn't write checkpoints at all yet (every version replays from JSON) |
| Row-data caching (LRU) | Metadata is cached; scanned rows are not |
| SQL-over-Flight | A real SQL text parser (`SqlQueryTranslator`) is done and wired into the CLI's `--sql` flag — Flight clients still build the `FlightQueryRequest` JSON shape directly; this is the natural next layer on top of that, not possible before it existed |
| Streaming/tailing subscriptions (`DoExchange`) | Push-based freshness for readers, not pull |

### Multi-format
| Item | Notes |
|---|---|
| Iceberg read support | ✅ Done (`labukdb-format-iceberg`) — read-only, path-based, local and remote (live-MinIO-verified), tested against a self-built fixture rather than a Spark/Trino-written table. Reachable via `--table`/`--sql`/Flight now too (auto-detected by probing for `_delta_log/` vs. `metadata/`), not just as a library call — see [USAGE.md](USAGE.md#delta-vs-iceberg) |
| `TableFormat` wired into `cli`/`flight` | ✅ Done for Iceberg's query path specifically (`DetectedFormat` + `QueryPlanner.planNormalized`) — Delta's own query path still bypasses it (`SnapshotManager`/`QueryPlanner.plan`/`ScanPlan`/`AddFile`, deliberately left unchanged; `Snapshot` is built from real commit-log actions in a way nothing can fake for Iceberg, so this was a genuine second path, not a shared-everything refactor) |
| Parquet-only tables (no transaction log) | Direct file listing with stats, no commit log |

### AI / LLM
| Item | Notes |
|---|---|
| MCP tool wrapper | `pyarrow.flight` client exposed as an MCP tool |
| Catalog semantic descriptions | Human-readable per-table description so `ListFlights` returns meaning, not just names |
| Case-insensitive filter option | LLM-generated queries often get casing wrong |

### Observability
| Item | Notes |
|---|---|
| Grafana as a Flight client | Grafana's Arrow Flight data source plugin needs no changes here — highest value, least new code |
| Prometheus metrics | Micrometer registry in `DeltaFlightProducer`: queries/sec, p95 latency, files-scanned vs. files-pruned |
| InfluxDB → Delta pipeline | Separate ingestion story, not something this engine does natively |

### Infrastructure
| Item | Notes |
|---|---|
| Distributed Flight / load balancing (reads) | Needs self-describing tickets — `TicketRegistry` is currently in-memory, single-instance |
| Distributed ingestion fleet | Router + sharded ingesters + a durable distributed WAL (not local-disk-per-node) + a catalog service tracking which node owns which table's live buffer. **Planned, not designed** — genuinely the largest remaining piece, closer to a distributed-systems project than an extension of the single-node write path; deliberately deferred until the single-node write path (above) is solid |
| Distributed query execution | Splitting one query's work across multiple compute nodes, not just client-side fan-out over `FlightEndpoint`s. **Planned, not designed** |
| Authentication / authorization | Token-based access control on Flight endpoints |
| Docker / Helm chart | Deployable as a service, not just a JAR |

---

*Copyright 2026 Afzal and lakehouse-flight contributors.
Licensed under the Apache License, Version 2.0 — see [LICENSE](LICENSE).*
