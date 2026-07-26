# delta-flight-engine

A Delta Lake table reader built from the transaction log up — no DuckDB,
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
- `Predicate` — the filter model: comparisons plus AND/OR (no generic NOT;
  see its javadoc for why).
- `PartitionPruner` — exact-match pruning from the log's partition values.
- `FileStatsPruner` — range pruning from `AddFile.stats` (min/max/null-count
  per column, if the writer collected them).
- `QueryPlanner` / `ScanPlan` — orchestrates both, reports files-skipped as
  a concrete metric.

All pruning is deliberately **fail-open**: anything it can't decide (missing
stats, non-partition column, type it doesn't recognize) is kept, never
skipped. Covered by `PrunerTest`, including the AND/OR combination logic and
the NEQ-never-prunes case.

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

**Scope limitation, stated plainly and worth checking against your own
data before pointing this at local-lakehouse-lab's output:** only flat
primitive columns are supported — INT32, INT64, FLOAT, DOUBLE, BOOLEAN,
BINARY (treated as UTF-8 string). Not yet supported: DECIMAL, nested/list
types, and — this one is a real, not hypothetical, concern — **INT96
timestamps**. Spark writes TIMESTAMP columns as INT96 by default; unless
your Spark job sets `spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS`
(INT64-encoded, which this reader does support), a column like `event_time`
from local-lakehouse-lab's own pipeline will hit
`ArrowSchemaMapper`'s `UnsupportedOperationException`. Worth checking your
writer config before running this against real tables.

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
`NoOpFlightProducer` matched arrow.version 16.1.0's actual API. Not yet
covered by its own test suite though (unlike log/planner/decode, `flight/`
has no tests exercising `getFlightInfo`/`getStream` against a real running
server yet) — that's the next real gap here, not a compile risk. `ListFlights`,
`DoPut`, `DoAction`, `DoExchange` are unimplemented (inherited as
"unimplemented" from `NoOpFlightProducer`) — genuinely out of scope for a
read-only single-table-at-a-time engine right now, not an oversight.

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
- `cache/` — intentionally last. Layers 1-4 need to be correct and measured
  against real data first; caching is a multiplier on a scan that's already
  cheap, not a fix for pruning that doesn't work yet.
- Real integration testing against local-lakehouse-lab's actual Spark
  output — everything above is tested against either hand-built fixtures or
  a file this project wrote itself. The real test is pointing
  `DeltaFlightServerApp` at a table `local-lakehouse-lab`'s Spark job
  produced and seeing what breaks.

## Module map

See the four-layer breakdown in the project architecture discussion —
`log/` → `planner/` → `decode/` → `flight/`, with `cache/` added once
1–4 are correct and measured, not before.

## Next

1. `mvn test` locally — fix whatever the compiler finds, starting with
   `flight/` if it fails there first (expected, see above).
2. Point `DeltaFlightServerApp` at a real table `local-lakehouse-lab`'s
   Spark job produced. Check the INT96 timestamp question first — it's the
   most likely first real-data failure.
3. `cache/` once 1-4 are proven against real data: snapshot cache (already
   half-built into `SnapshotManager`), Parquet footer cache, optionally a
   decoded-batch LRU for hot files. Safe to cache indefinitely per file
   because Delta files are immutable once written — no invalidation logic
   needed, unlike caching rows in a mutable table.
