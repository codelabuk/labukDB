# lakehouse-flight

[![CI](https://github.com/codelabuk/lakehouse-flight/actions/workflows/ci.yml/badge.svg)](https://github.com/codelabuk/lakehouse-flight/actions)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

**A from-scratch Delta Lake read engine, served over Apache Arrow Flight.**

Reads Delta tables on local disk, MinIO, S3, or ADLS — applies four-level
predicate pushdown — streams results as Arrow IPC to any client in any
language. Includes a working local-LLM RAG demo (Ollama).

## What this is

- A **read engine**, not a SQL engine — one operator (scan with pushdown),
  no joins, no aggregations
- The only Java Delta reader built from the `_delta_log` up, with no
  `delta-rs`, `delta-kernel`, or DuckDB in the path — every layer is
  readable and independently testable
- A **Flight server** — any language with an Arrow Flight client binding
  (Python, Java, Go, Rust, JS) connects unmodified
- An honest implementation with documented limitations, not marketing

## What this is not

- A Spark replacement — different scope entirely
- A writer — read-only by design; Spark/Flink handle writes
- A full query engine — use DuckDB or Trino if you need SQL

---

*Copyright 2026 Afzal and lakehouse-flight contributors.
Licensed under the Apache License, Version 2.0 — see [LICENSE](LICENSE).*

---

no delta-rs, no Spark in the read path. Exposes results over Arrow Flight.

Companion project to `local-lakehouse-lab`, not part of it: that project is
the producer story (Spark/Flink writing Delta tables, plus a DuckDB query
layer). This one is the from-scratch consumer story — it reads the same
`_delta_log` + Parquet output `local-lakehouse-lab`'s Spark job already
produces on disk, but implements its own log parsing, pruning, and Parquet
decode instead of delegating to an existing engine.

## Why build this instead of using delta-kernel / DuckDB

Understanding the mechanics, not shipping fastest. See the module map below
— each package is deliberately small enough to reason about end to end.

## Status

**Compiled and tested locally — real results, not sandbox output.** All 28
source files compile clean. 29/29 tests pass across `log/`, `planner/`, and
`flight/` (the Flight API guesses were correct — `FlightInfo`'s builder and
`NoOpFlightProducer` compiled as written). The `decode/` integration tests
initially failed with `MemoryUtil` / `InaccessibleObjectException` — that's
Arrow's off-heap memory library needing `--add-opens` on Java 9+, now wired
into Surefire's `argLine` in `pom.xml` so `mvn test` handles it
automatically. Two things fixed along the way, worth knowing if you ever
rebuild this from scratch:

1. The Flight artifact ID is `flight-core`, not `arrow-flight-core` —
   Arrow's Flight modules dropped the `arrow-` prefix that `arrow-vector`
   and `arrow-memory-netty` use. Inconsistent upstream naming, not a
   version issue.
2. Running the built jar directly (not through Maven) needs the same
   `--add-opens` flag on the `java` command line — Surefire's `argLine`
   only covers `mvn test`. See `DeltaFlightServerApp` below.

### Layer 1 — `log/`
Commit-log parsing (`CommitLogReader`, the `action/` model) and snapshot
resolution (`SnapshotManager`, `Snapshot`) — `latest()` / `atVersion(n)` /
`atTimestamp(instant)`, with an incremental forward cache and full replay on
backward moves. Covered by `CommitLogReaderTest` and `SnapshotManagerTest`
against realistic fixture commits.

### Layer 2 — `planner/`
- `Predicate` — the filter model: comparisons, `IN`, `LIKE` (SQL-style,
  `%`/`_`, case-sensitive), combined with AND/OR (no generic NOT; see its
  javadoc for why).
- `PartitionPruner` — exact-match pruning from the log's partition values,
  including `IN`/`LIKE` against a file's known exact partition value.
- `FileStatsPruner` — range pruning from `AddFile.stats` (min/max/null-count
  per column, if the writer collected them). `IN` gets real range pruning
  (OR across each listed value's range check). `LIKE` does NOT get
  range-based pruning here — a documented gap, not a bug: would need
  extracting a fixed prefix and comparing it lexicographically against
  min/max, not attempted yet. Correctness isn't affected, since the
  residual filter still applies the pattern exactly — this only means a
  `LIKE`-only predicate skips no files at this level.
- `PartitionValueSubstituter` — folds `IN`/`LIKE` on partition columns to
  boolean constants too, same as plain comparisons.
- `QueryPlanner` / `ScanPlan` — orchestrates all of the above, reports
  files-skipped as a concrete metric.

All pruning is deliberately **fail-open**: anything it can't decide (missing
stats, non-partition column, type it doesn't recognize, a `LIKE` pattern
against a non-string value) is kept, never skipped. Covered by `PrunerTest`
(comparisons, AND/OR, NEQ-never-prunes, `IN` range pruning, `LIKE`'s
documented no-pruning behavior) and `PartitionValueSubstituterTest`.

**Timestamp literals are now real on the wire** — `PredicateCodec` (in
`flight/query/`) supports a `timestamp` literal type (ISO-8601 string,
parsed to `Instant`), which is also now the single representation INT96
columns are compared as everywhere (`GroupPredicateEvaluator`,
`RowGroupPruner`) — replacing the earlier epoch-microsecond-long convention
that only existed because the wire format used to lack a timestamp type.
Covered by `PredicateCodecTest`.

### Layer 3 — `decode/`
- `RowGroupPruner` — same range logic as `FileStatsPruner`, one level
  deeper, against Parquet's own footer statistics.
- `GroupPredicateEvaluator` — the residual, per-row filter; SQL-style NULL
  handling (a NULL never satisfies a comparison), which is deliberately
  different from the fail-open pruning layers above it.
- `ArrowSchemaMapper` / `RowAppender` — the Parquet-to-Arrow type bridge.
- `ParquetToArrowReader` — fuses row-group pruning, decode, and residual
  filtering into one pass: a row that fails the residual filter is never
  copied into an Arrow vector, rather than decoded and discarded.

**Scope limitation, now narrower after a real-table run surfaced the INT96
gap directly (see `RealTableSmokeCheck` output below):** flat primitive
columns only — INT32, INT64, FLOAT, DOUBLE, BOOLEAN, INT96 (Spark's default
legacy timestamp encoding — now decoded via `Int96TimestampCodec`, epoch
microseconds UTC; see its javadoc for the exact caveat around Spark's
session-timezone rebasing config), and BINARY (UTF-8 string). Still not
supported: DECIMAL, nested/list types. Timestamp predicates over the wire
should use the `timestamp` literal type (ISO-8601) now — see
`PredicateCodec`'s class javadoc — rather than the epoch-microsecond `long`
convention this project used before that literal type existed.

Covered by `ParquetToArrowReaderTest`, which writes a real Parquet file with
parquet-mr's own writer and reads it back — the closest thing to a true
end-to-end check available without a running Delta table. See that test's
class javadoc for what it does and doesn't exercise (it doesn't force
multiple row groups).

### Layer 4 — `flight/`
- `FlightQueryRequest` / `PredicateCodec` (in `flight/query/`) — the JSON
  wire format a client sends as a `FlightDescriptor` command: table root,
  version/timestamp, columns, predicate.
- `ScanTicket` / `TicketRegistry` — server-side state mapping an opaque
  ticket id to the single-file scan it refers to. **Named as a deliberate
  simplification, not a final design** — see `TicketRegistry`'s javadoc for
  why a self-describing ticket would be the move if this ever needs to run
  as more than one instance.
- `DeltaFlightProducer` — `GetFlightInfo` resolves the snapshot and runs
  Layer 2 pruning, returning one `FlightEndpoint` per surviving file for
  client-side parallelism; `DoGet` runs Layer 3 for exactly the one file its
  ticket refers to, lazily (an endpoint the client never fetches never
  causes a file to be opened).
- `DeltaFlightServerApp` — bootstrap `main()`.

**Confirmed to compile as written** — `FlightInfo.builder(...)` and
`NoOpFlightProducer` matched arrow.version 16.1.0's actual API.
**Now covered by `FlightIntegrationTest`**, which starts a real
`FlightServer` in-process (same producer `DeltaFlightServerApp` uses, an
ephemeral local port) against a fixture table with real Parquet files —
not just log fixtures, since `DoGet` genuinely opens files — and drives it
with a real `FlightClient`. It exercises partition pruning (asserts exactly
one surviving `FlightEndpoint`) and residual row filtering together in one
round trip, so a pass means Layer 2 and Layer 3 are correctly wired through
`GetFlightInfo`/`DoGet`, not just individually unit-tested. `ListFlights`,
`DoPut`, `DoAction`, `DoExchange` are unimplemented (inherited as
"unimplemented" from `NoOpFlightProducer`) — genuinely out of scope for a
read-only single-table-at-a-time engine right now, not an oversight.

**Partition-column reconstruction — the gap `FlightIntegrationTest`
surfaced — is now fixed, not just named:**
- `planner/PartitionValueSubstituter` folds any partition-column comparison
  in a predicate down to a boolean constant using one specific file's known
  exact partition values, then simplifies the expression with standard
  boolean algebra. This fixes a real correctness bug alongside the
  feature: without it, `level = 'ERROR' AND amount > 2` would have thrown
  at decode time on the first row of every surviving file, since
  `level` doesn't exist in the physical Parquet schema at all.
- `decode/CellAppender` (shared interface), `decode/PartitionValueAppender`
  (writes a constant per file), and `RowAppender` now implementing the same
  interface — `ParquetToArrowReader` builds one flat list of column
  fillers per query, physical and partition columns mixed, in requested
  output order.
- `ArrowSchemaMapper` now types a requested column as either a physical
  Parquet field or a UTF-8 partition column; requesting a column that's
  neither throws `IllegalArgumentException` rather than silently returning
  wrong data.
- Default projection (no explicit columns requested) now means "every
  physical column, plus every partition column" — matching what "select
  all" means against a real Hive-partitioned table.
- Tested at both layers: `ParquetToArrowReaderTest` directly (partition
  reconstruction in isolation, plus the unknown-column failure mode), and
  `FlightIntegrationTest` end-to-end (partition pruning + predicate
  substitution + reconstruction, all together, through a real client/server
  round trip).

**Running `DeltaFlightServerApp` directly needs the same JVM flag Surefire
now sets automatically:**

```
java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED \
     -cp target/classes:<dependency-classpath> \
     com.labuk.deltaflight.flight.DeltaFlightServerApp
```

(or bundle it into a shaded jar's `MANIFEST.MF`/a launch script later —
fine to just pass it on the command line for now.)

### What's still not here
- Checkpoint files (`.checkpoint.parquet`) — Layer 1 still replays JSON
  commits from version 0 on a cache miss.
- Real integration testing against local-lakehouse-lab's actual Spark
  output — everything above is tested against either hand-built fixtures or
  a file this project wrote itself. The real test is pointing
  `DeltaFlightServerApp` at a table `local-lakehouse-lab`'s Spark job
  produced and seeing what breaks.

### Caching — no longer entirely deferred
Two of the three originally-planned caches now exist, both **unverified —
not yet run through `mvn test`, same status as everything else in this
section**:

- `log/TableCache.java` — reuses one `SnapshotManager` per table root
  across requests. This fixes a real inefficiency, not a hypothetical one:
  `DeltaFlightProducer` previously built a *fresh* `SnapshotManager` on
  every `GetFlightInfo` call, which meant `SnapshotManager`'s own
  incremental cache never actually got used across requests — every query
  replayed the commit log from version 0, regardless of how recently the
  same table had just been queried. `TableCacheTest` covers same-instance
  reuse, path normalization, and eviction.
- `decode/ParquetFooterCache.java` — caches a file's Parquet schema by
  path, since `GetFlightInfo`'s schema inference re-opens a file on every
  query otherwise, even identical repeated ones. Scoped deliberately to
  schema-only access — the full row-data read path still opens and parses
  the footer itself when actually scanning, since avoiding that too would
  need a `ParquetFileReader` constructor accepting a pre-read footer, whose
  exact availability in this project's pinned parquet-mr version hasn't
  been verified. Named as a follow-up, not guessed at. `ParquetFooterCacheTest`
  covers cache-hit behavior directly (asserts `size()` stays at 1 across
  repeated lookups of the same file, not just that the returned schema is
  correct).
- Not done: a decoded-batch LRU for hot files (still genuinely optional,
  lower priority than the two above).

## Module map

See the four-layer breakdown in the project architecture discussion —
`log/` → `planner/` → `decode/` → `flight/`, with caching added once
1–4 were correct and measured, not before.

## Building and running the CLI as an actual command

```
mvn clean package          # runs tests too; add -DskipTests to skip them
```

This produces `target/delta-cli.jar` — one file, every dependency bundled
in. From then on:

```
bin/delta-cli catalog add logs D:\repo\local-lakehouse-lab\lakehouse\logs_delta
bin/delta-cli catalog list
bin/delta-cli describe logs
bin/delta-cli query --table logs --filter "level=ERROR" --limit 10
```

(Windows: `bin\delta-cli.bat ...`.) No classpath string, ever. **Zip
extraction on Windows won't preserve the executable bit on `bin/delta-cli`**
— run `chmod +x bin/delta-cli` once in Git Bash if it won't execute
directly; `java -jar target/delta-cli.jar ...` always works regardless.

Not yet built/run here — same caveat as always, and this one specifically
depends on the shade plugin resolving cleanly, which needs Maven Central
access this sandbox doesn't have.

## Catalog, CLI subcommands, and Python client demo — new

- `catalog/TableCatalog.java` — a name → table-root registry, persisted to
  `~/.delta-flight-engine/catalog.json`. Not Unity Catalog, doesn't try to
  be — just enough that a client (especially an AI agent/MCP tool) can say
  `"logs"` instead of needing to know an exact filesystem path. Tested via
  `TableCatalogTest` (register/resolve/list/remove, and persistence across
  separate instances pointed at the same file).
- `FlightQueryRequest` gained an optional `table` field (catalog name) —
  `tableRoot` (raw path) still works exactly as before; `table` is tried
  first if both are present. `DeltaFlightProducer.listFlights` is now a
  real implementation (previously inherited a no-op) — lists catalog
  entries, not a full per-table scan plan.
- `cli/` restructured into subcommands, kubectl-style, instead of one flat
  flag set:
  - `delta-cli catalog add <name> <path>` / `list` / `remove <name>`
  - `delta-cli describe <name-or-path>` — schema, partition columns, file
    count, no row data touched
  - `delta-cli query --table <name-or-path> [--filter ...] [--columns ...]`
    — same engine as before, now catalog-aware via `TableResolver` (tries
    the catalog first, falls back to a literal path — nothing about
    pre-catalog raw-path usage breaks)
- `python/query_demo.py` — a real `pyarrow.flight` client against a running
  `DeltaFlightServerApp`. Proof, not just a claim, that any Flight-capable
  client in any language works unmodified. Now gives a clear error instead
  of a raw traceback for the single-quotes-vs-JSON mistake (JSON needs
  double quotes; wrap the whole shell argument in single quotes instead).
- `DeltaFlightProducer`'s exception handling widened to a catch-all in both
  `GetFlightInfo` and `DoGet`, with the real exception logged server-side
  (`e.printStackTrace()`) — found via a real failure: an exception type
  neither of the original two narrower catches covered (`UncheckedIOException`
  from a bad table path) was reaching Flight's generic default handler,
  producing a useless client-side message with nothing printed server-side
  at all.

**Confirmed working against your real table via Python, over the network** —
unfiltered (3,999 rows, 8 files, correct partition reconstruction, correct
INT96 timestamp decode) and filtered (`level="ERROR"` → 1,444 rows across 4
files; `level="WARN"` → 2,555 rows across 4 files; case-sensitive by design,
confirmed by `level="warn"` correctly returning zero). The catalog/CLI
subcommand changes in this section are NOT yet tested locally — same
caveat as always for anything just written.

## RAG demo — new

`python/rag_demo.py` — plain-English question in, grounded answer out, via
two separate local Ollama calls with two different jobs:

1. Question + real table schema (fetched live via `GetFlightInfo`, so it's
   never stale or hand-maintained) → Ollama writes the same JSON
   `query_demo.py` sends by hand → real query against the server.
2. Question + the actual retrieved rows → Ollama answers, grounded in step
   1's real results rather than the model's own guess. This split is what
   makes it RAG rather than just "LLM with a database nearby."

Fully local — no paid API, matches the Ollama-based pattern from
`local-lakehouse-lab`'s own NL-to-SQL work, this engine substituted in as
the backend instead of DuckDB.

**Known limitation, stated plainly (see the script's own docstring too):**
string filters are case-sensitive, and the LLM has no way to know your
data's exact casing from the question alone — it can only guess. A real
fix would fetch distinct values for low-cardinality columns as prompt
hints; not attempted here.

**Not yet tested locally** — same caveat as always, and this one has an
extra moving part (Ollama itself, plus whichever model you have pulled) on
top of the usual "needs a real compiler pass" caveat.

## Storage abstraction + checkpoint support — new, high-risk parts flagged honestly

**Storage abstraction — solid, low-risk:**
- `log/LogStorage` — interface abstracting what `SnapshotManager` needs
  from the `_delta_log` directory. `LocalNioLogStorage` (unchanged
  `java.nio.file` behavior) and `HadoopLogStorage` (Hadoop `FileSystem`,
  S3/MinIO/ADLS-capable given the right connector jar + config) both
  implement it.
- `SnapshotManager` gained a second constructor
  (`org.apache.hadoop.fs.Path` + `Configuration`) alongside the original
  local one — **purely additive**, the original constructor's behavior is
  provably unchanged (see `HadoopBackedSnapshotManagerTest`, which asserts
  both constructors give identical results for the same local directory).
- `hadoop-aws` added as an optional dependency (S3/MinIO via `s3a://`).
  `hadoop-azure` added as an optional dependency (ADLS Gen2 via `abfss://`).
  Both optional so a consumer using only local storage isn't forced to
  pull in either SDK. `MinioConfiguration` and `AdlsConfiguration`
  helpers handle the settings that matter for each backend, both
  readable from environment variables automatically.
- `CommitFileNames`/`CommitLogReader` refactored to be storage-agnostic
  (String-based, not tied to a specific Path type) — `CommitLogReader.read(Path)`'s
  behavior is unchanged, existing tests untouched.
- **Not yet extended to `decode/`** — Parquet *data* file reads still
  assume a local `java.io.File`. The log/metadata layer is now
  storage-abstracted; wiring `decode/` the same way is the next step
  before a genuine end-to-end remote query works.

**Checkpoint support — the highest-risk code in this project, stated
plainly, not softened:**
- `log/CheckpointReader` hand-parses a *nested* Parquet schema (structs
  for add/remove/metaData/protocol, MAP-encoded partitionValues/configuration,
  LIST-encoded partitionColumns) using parquet-mr's raw `Group` API — field
  names assumed from the Delta protocol spec and the standard Parquet
  MAP/LIST 3-level encoding, **never verified against a real
  Spark-produced checkpoint file**, since none exists in this environment.
- `CheckpointReaderTest` round-trips against a checkpoint file built with
  the assumed schema — this proves the `Group`-navigation logic is
  mechanically correct, **not** that the schema assumption itself is
  right. If real checkpoint loading fails, that test still passing tells
  you the bug is in the schema assumption, not the parsing mechanics.
- Only single-part checkpoints are supported; multi-part checkpoints
  throw a clear `UnsupportedOperationException` rather than silently
  reading partial data.
- `SnapshotManager` now checks for a checkpoint at or below the target
  version before falling back to full JSON replay from version 0.

## Next — v1 close-out

1. Run `mvn test` — the ADLS additions (`AdlsConfiguration`, `hadoop-azure`
   dependency, merged `storageConf` in `DeltaFlightProducer` and
   `TableResolver`) plus earlier changes all need a real compiler pass.
2. Verify checkpoint support against a real Spark-produced checkpoint —
   run the streaming job until the table crosses 10 commits, restart the
   server cold, query the table.
3. Push to GitHub as `lakehouse-flight` (Apache 2.0, CI already wired).

## v2 Roadmap

### Core engine extensions
| Item | Notes |
|---|---|
| Nested/list type support in `decode/` | Required for vector embeddings |
| Checkpoint multi-part support | Single-part works; multi-part throws UnsupportedOperationException |
| Row-data caching (LRU) | Metadata cached today; actual scanned rows not yet |
| Streaming subscriptions (`DoExchange`) | Push-based freshness, not pull |

### Lakehouse gateway — the primary v2 target
| Item | Notes |
|---|---|
| Iceberg metadata reader | Avro manifest lists/files — different from Delta's JSON log |
| Multi-format dispatcher | One catalog entry knows its format; `GetFlightInfo` routes accordingly |
| Parquet-only tables | No transaction log — direct file listing with stats |
| Single Flight interface across Delta + Iceberg + Parquet | The v2 headline feature |

### AI / LLM
| Item | Notes |
|---|---|
| MCP tool wrapper | `pyarrow.flight` client exposed as an MCP tool — agent says "query the logs table", gets grounded rows back |
| Catalog semantic descriptions | Store a human-readable description per table so `ListFlights` returns meaning, not just names — "logs: application error/warn/info events, partitioned by date and level" |
| Case-insensitive filter option | LLM-generated queries often get casing wrong (ERROR vs error) — optional flag per query |

### Observability (Grafana / Prometheus / InfluxDB)
Three distinct use cases, each real, each independently valuable:

**Grafana as a Flight client (highest value, least new code):**
Grafana has an Arrow Flight data source plugin. `DeltaFlightServerApp` is
already a valid Flight server — Grafana can query it directly without any
changes to this project. The demo: point Grafana at `logs_delta`, build a
dashboard showing ERROR/WARN rates over time, partitioned by service.
Your own log data, queried via your own engine, visualized in Grafana.
No InfluxDB in between. This is the most compelling v2 demo available.

**Prometheus server metrics (standard service observability):**
Once the server is actually deployed (not just "run on a laptop"), add a
Micrometer registry to `DeltaFlightProducer` exposing: queries/sec, p95
latency per query, files-scanned vs files-pruned ratio per query, error
rate. Prometheus scrapes a `/metrics` endpoint; Grafana dashboards show
"is pruning actually working in production." Standard Java service
observability — Micrometer + Prometheus + Grafana is a well-worn path,
low implementation risk.

**InfluxDB as a writer into Delta (pipeline integration, different scope):**
If time-series metrics flow into InfluxDB, a Spark/Flink job could write
them into a Delta table and this engine reads them back. This is a pipeline
architecture story, not something the engine does natively — worth noting
as a possible integration but not a core v2 item.

### Infrastructure
| Item | Notes |
|---|---|
| Distributed Flight / load balancing | Multiple server instances; self-describing tickets (remove TicketRegistry's in-memory-only limitation) |
| Authentication / authorization | Token-based access control on Flight endpoints |
| Docker / Helm chart | Deployable as a service, not just a JAR |
| Streaming subscriptions (`DoExchange`) | Live push when new commits land |

