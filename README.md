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

**Layer 1 (log parsing + snapshot resolution) — this commit.** Not
sandbox-compiled: this environment can't reach Maven Central
(`repo.maven.apache.org` is blocked by the network policy here) and has no
`javac`, so nothing below has been built or run. Written against the real
Delta commit-JSON shape (verified against actual Delta OSS commit files),
but you're the first real compiler pass — run `mvn test` locally before
trusting it.

What's in this commit:

- `log/action/` — the action model: `AddFile`, `RemoveFile`,
  `MetadataAction`, `ProtocolAction`, `CommitInfoAction`, wrapped by
  `SingleAction` (one JSON-lines entry = one action).
- `log/CommitLogReader.java` — parses a single commit file
  (`_delta_log/NNNNNNNNNNNNNNNNNNNN.json`) into its list of actions.
- `log/CommitFileNames.java` — the 20-digit zero-padded filename convention.
- `log/Snapshot.java` — an immutable, defensively-copied live file set +
  schema as of one version. This is what a query scans against.
- `log/SnapshotManager.java` — replays commits into a `Snapshot`, with an
  incremental forward cache (cheap for a streaming table's frequent small
  commits) and a full-replay fallback when asked for an older version than
  what's cached. Supports `latest()`, `atVersion(n)`, `atTimestamp(instant)`.
- Fixture commits under `src/test/resources/` — a table creation with two
  adds (`level=ERROR`, `level=INFO` partitions), then a delete removing the
  ERROR file — mirrored under `sample-table/_delta_log/` for
  `SnapshotManagerTest`, which specifically covers: the removed file
  disappearing from the live set (without the physical file being touched),
  metadata carrying forward across commits that don't redeclare it,
  timestamp-based resolution, and — the trickiest one — that resolving an
  *older* version after the cache has already moved forward still returns
  the correct (larger) file set, not an incorrectly-diffed one.

What's deliberately NOT handled yet:

- Checkpoint files (`.checkpoint.parquet`) — commit-by-commit JSON replay
  only, for now. Fine for a young/low-volume table, not for one with
  thousands of commits.
- `txn`, `cdc`, `domainMetadata` actions — ignored via
  `@JsonIgnoreProperties(ignoreUnknown = true)`. Not needed for a read-only
  engine targeting basic append/overwrite tables.
- Concurrent writer detection / conflict resolution — this is a read-only
  engine; it consumes whatever the log says, it doesn't need to arbitrate
  between competing writers the way Delta's own optimistic concurrency
  control does on the write side.

## Module map

See the four-layer breakdown in the project architecture discussion —
`log/` → `planner/` → `decode/` → `flight/`, with `cache/` added once
1–4 are correct and measured, not before.

## Next

`planner/` (Layer 2): partition pruning from the log's partition values,
file-level pruning from `AddFile.stats`, row-group pruning from the Parquet
footer once we get to `decode/`. This is the layer that turns "give me all
active files" into "give me only the files that could possibly match."
