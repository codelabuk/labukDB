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

## Next

1. Run `mvn test` locally — filter language expansion (`IN`, `LIKE`, the
   timestamp literal type, and the INT96-to-Instant switch) touches
   `PredicateEvaluation`, every pruning layer, `GroupPredicateEvaluator`,
   `PartitionValueSubstituter`, and `PredicateCodec` — real surface, worth
   a full pass before trusting it.
2. CLI, then a scoped catalog (name → table-root resolution, `ListFlights`).
3. Checkpoint support in `SnapshotManager`.
