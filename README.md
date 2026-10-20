# lakehouse-flight

[![CI](https://github.com/codelabuk/lakehouse-flight/actions/workflows/ci.yml/badge.svg)](https://github.com/codelabuk/lakehouse-flight/actions)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

**A from-scratch Delta Lake read engine, served over Apache Arrow Flight.**

It reads Delta tables — local disk, S3, MinIO, or ADLS — prunes files down
using a four-level pushdown pipeline, and streams the surviving rows out as
Arrow IPC to any client in any language. No `delta-rs`, no `delta-kernel`,
no DuckDB in the read path: the transaction log, the pruning, and the
Parquet decode are all implemented here, one small and independently
testable layer at a time.

For a full guided tour of how it works, see **[ARCHITECTURE.md](ARCHITECTURE.md)**.
This README covers what the project is, how the pieces fit together at a
glance, and how to build and run it.

---

## What this is

- **A read engine, not a SQL engine.** One operator — scan with pushdown.
  No joins, no aggregations.
- **Built from `_delta_log` up.** Every layer (log parsing, pruning,
  Parquet decode, Flight serving) is its own package, readable and tested
  on its own.
- **A real Flight server.** Any language with an Arrow Flight client
  binding — Python, Java, Go, Rust, JS — talks to it unmodified.
- **Honest about its gaps.** See [Status & known limitations](#status--known-limitations)
  below rather than a marketing pass.

## What this is not

- **Not a Spark replacement** — different scope entirely.
- **Not a writer** — read-only by design; Spark/Flink handle writes.
- **Not a full query engine** — reach for DuckDB or Trino if you need SQL.

Companion project to `local-lakehouse-lab` (not part of it): that project
is the *producer* story — Spark/Flink writing Delta tables, plus a DuckDB
query layer. This one is the from-scratch *consumer* story, reading the
same `_delta_log` + Parquet output with its own log parsing, pruning, and
decode instead of delegating to an existing engine.

## Why build this instead of using delta-kernel / DuckDB

The goal is understanding the mechanics, not shipping fastest. Each layer
below is deliberately small enough to reason about end to end.

---

## The four layers

```
Writers (Spark batch, Spark streaming, Flink) → Delta table
                                                    │
   ┌────────────────────────────────────────────────┴──┐
   │  1. log/       parse the log, resolve a snapshot   │
   │  2. planner/   decide which files to even open     │
   │  3. decode/    Parquet → Arrow, filter rows         │
   │  4. flight/    serve over gRPC                      │
   └───────────────────────────────────────────────────┘
                          │
        Python / Java / LLM / dashboards / MCP
```

| Layer | Package | Job | Key types |
|---|---|---|---|
| 1 | `log/` | Parse the commit log, resolve a snapshot at a version, timestamp, or `latest()`. Incrementally cached; falls back to checkpoints where available. | `CommitLogReader`, `SnapshotManager`, `CheckpointReader` |
| 2 | `planner/` | Decide which files can be skipped, without opening them, using partition values and file-level stats from the log. Fail-open: anything it can't decide, it keeps. | `Predicate`, `PartitionPruner`, `FileStatsPruner`, `QueryPlanner` |
| 3 | `decode/` | For each surviving file: prune row groups via Parquet footer stats, decode only the requested columns, apply the residual filter row-by-row, and reconstruct partition columns as constants. Decode and filter are fused into one pass. | `RowGroupPruner`, `GroupPredicateEvaluator`, `ParquetToArrowReader` |
| 4 | `flight/` | Serve it. `GetFlightInfo` plans and returns one `FlightEndpoint` per surviving file; `DoGet` lazily decodes exactly the file its ticket refers to. | `DeltaFlightProducer`, `LabukDbServerApp` |

Supporting packages: `storage/` (local/S3/MinIO/ADLS location resolution),
`catalog/` (name → table-root registry), `cli/` (the `labukdb` tool).

**Multi-module since the labukDB restructuring:** these four layers now
live in separate Maven modules — `labukdb-core` (log/planner/decode/
storage/catalog, no Flight dependency, embeddable like DuckDB),
`labukdb-flight` (Flight serving), `labukdb-server-app` (the runnable
server jar), `labukdb-cli` (the `labukdb` binary, depends on
`labukdb-core` only). See
[docs/labukdb-v2-architecture.md](docs/labukdb-v2-architecture.md) for
the full module design and what's planned on top of it (write support,
Iceberg, etc.).

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

---

## The catalog

`TableCatalog` maps a short name to a table location, persisted to
`~/.labukdb/catalog.json` — so a client (especially an AI agent or MCP
tool) can say `"logs"` instead of needing to know an exact path. Not
Unity Catalog, and doesn't try to be.

## Storage backends

Local disk, S3/MinIO (`s3a://`, via optional `hadoop-aws`), and ADLS Gen2
(`abfss://`, via optional `hadoop-azure`) are all resolved through one
entry point, `TableLocation`, by URI scheme. Both dependencies are
optional — a local-only consumer isn't forced to pull in either SDK.

**Read path:** fully remote-capable — log/metadata and the Parquet data
read in `decode/` both dispatch through `TableLocation`, the same path
`DeltaFlightProducer` uses for every query.

**Write path:** the commit log's atomic append (`CommitWriter`'s
underlying `createCommitFile`) is remote-capable for all three backends
and **verified against a real MinIO cluster** (not just implemented —
see Status below). The Parquet *data file* write (`ArrowToParquetWriter`)
is still local-disk only — writing a new table version against a remote
table doesn't fully work end-to-end yet.

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

**Type support (`decode/`):** INT32, INT64, FLOAT, DOUBLE, BOOLEAN, BINARY
(UTF-8), INT96 (Spark's legacy timestamp encoding, decoded via
`Int96TimestampCodec`), and `LIST<float/double/int/long/string>` for flat
vector/array columns. Not yet supported: DECIMAL, nested `LIST<LIST<...>>`,
MAP, STRUCT.

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

**Flight surface:** `GetFlightInfo`, `DoGet`, and `ListFlights` are
implemented. `DoPut`, `DoAction`, `DoExchange` are unimplemented —
genuinely out of scope for a read-only, single-table-at-a-time engine
right now.

---

## Built on

Libraries this project actually depends on (see `pom.xml` for exact
versions):

| Library | Used for |
|---|---|
| [Apache Parquet (`parquet-mr`)](https://parquet.apache.org/) | Footer statistics, row-group reads, the `Group` record API `GroupPredicateEvaluator` and `CheckpointReader` decode against |
| [Apache Arrow](https://arrow.apache.org/) | The in-memory columnar format every decoded row lands in (`arrow-vector`) |
| [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) | The gRPC-based wire protocol `flight/` serves over — this is *why* any language's Flight client works unmodified |
| [Apache Hadoop](https://hadoop.apache.org/) (`FileSystem` API only) | Storage abstraction for `log/` and `decode/` — no MapReduce/YARN/HDFS daemons involved, see [HADOOP_USAGE.md](HADOOP_USAGE.md) |
| [Jackson](https://github.com/FasterXML/jackson) | Parsing the `_delta_log` JSON commit files |

## Inspired by, deliberately not depending on

The point of this project is understanding the mechanics of a Delta reader
first-hand, so these were read as references, not pulled in as
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

### Core engine
| Item | Notes |
|---|---|
| Nested `LIST<LIST<...>>`, MAP, STRUCT | `LIST<primitive>` already supported |
| Checkpoint multi-part support | Single-part works today |
| Row-data caching (LRU) | Metadata is cached; scanned rows are not |
| Streaming subscriptions (`DoExchange`) | Push-based freshness, not pull |
| Remote Parquet reads in `decode/` | Storage abstraction stops at the log layer today |

### Lakehouse gateway (primary next target)
| Item | Notes |
|---|---|
| Iceberg metadata reader | Avro manifest lists/files — different from Delta's JSON log |
| Multi-format dispatcher | One catalog entry knows its format; `GetFlightInfo` routes accordingly |
| Parquet-only tables | No transaction log — direct file listing with stats |
| Single Flight interface across Delta + Iceberg + Parquet | The headline feature |

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
| Distributed Flight / load balancing | Needs self-describing tickets — `TicketRegistry` is currently in-memory, single-instance |
| Authentication / authorization | Token-based access control on Flight endpoints |
| Docker / Helm chart | Deployable as a service, not just a JAR |

---

*Copyright 2026 Afzal and lakehouse-flight contributors.
Licensed under the Apache License, Version 2.0 — see [LICENSE](LICENSE).*
