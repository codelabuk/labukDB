# Architecture walkthrough

> **Note:** this document describes the v1 single-module engine
> (package `com.labuk.deltaflight`). The project has since been split
> into the multi-module labukDB layout (package `com.labuk.labukdb`,
> modules `labukdb-core`/`labukdb-flight`/`labukdb-server-app`/`labukdb-cli`)
> — see [docs/labukdb-v2-architecture.md](docs/labukdb-v2-architecture.md)
> for the current module design. The four-layer concepts below (log →
> planner → decode → flight) are unchanged, just repackaged; this doc
> gets a full rewrite once the write-path work lands on top of them.

A complete tour of what this project is and how every piece works, meant
to be readable start to finish. The README covers status and caveats; this
covers understanding.

---

## What this is, in one paragraph

A Delta Lake **read engine**, built from the transaction log up — no
DuckDB, no delta-rs, no Spark in the read path — that answers filtered,
projected queries fast and serves results over Arrow Flight, so any
client in any language can consume them. It is not a SQL engine (no
joins, no aggregations); it implements one operator, the table scan with
pushdown, and implements it carefully.

---

## The core idea: a table is a log, not a folder

The single most important concept. A Delta table looks like a folder of
Parquet files, but the folder is **not** the source of truth — the
`_delta_log/` directory is. It contains numbered JSON commit files:

```
logs_delta/
├── _delta_log/
│   ├── 00000000000000000000.json      ← commit 0
│   ├── 00000000000000000001.json      ← commit 1
│   └── 00000000000000000010.checkpoint.parquet
├── log_date=2026-07-17/
│   └── level=ERROR/part-0.snappy.parquet
└── ...
```

Each commit file is JSON-lines; each line is one **action**:

| Action | Meaning |
|---|---|
| `add` | this file is now part of the table |
| `remove` | this file is no longer part of the table (from this version on) |
| `metaData` | the table's schema and partition columns |
| `protocol` | minimum reader/writer version required |
| `commitInfo` | who wrote this and when (not needed for correctness) |

To know what the table contains at version N, you replay commits 0..N,
applying adds and removes. That reconstruction is the whole job of
Layer 1.

**Why `remove` doesn't delete anything:** a removed file's bytes stay on
disk until a separate VACUUM operation cleans them up. That's exactly
what makes time travel possible — reading version 5 can legally use a
file that version 6 removed.

---

## The four layers

```
Writers (Spark batch, Spark streaming, Flink) → Delta table
                                                    │
   ┌────────────────────────────────────────────────┴──┐
   │  1. log/       parse the log, resolve a snapshot   │
   │  2. planner/   decide which files to even open     │
   │  3. decode/    Parquet → Arrow, filter rows        │
   │  4. flight/    serve over gRPC                     │
   └───────────────────────────────────────────────────┘
                          │
        Python / Java / LLM / dashboards / MCP
```

### Layer 1 — `log/`: what does this table contain?

**`CommitLogReader`** parses one commit file into a list of actions.

**`SnapshotManager`** replays commits into a **`Snapshot`** — the live
file set plus schema at one specific version. Three ways to ask:
`latest()`, `atVersion(n)`, `atTimestamp(instant)`.

Two things worth understanding here:

- **Isolation** — a query resolves its snapshot *once*, then scans that
  fixed file list for its entire execution. `Snapshot` defensively copies
  the file set at construction, so writers committing new versions
  mid-query cannot change what that query sees. This is the one ACID
  property this engine actually implements; the others come from the
  writer.
- **Incremental caching** — replaying from version 0 on every query would
  be wasteful for a streaming table getting a commit every few seconds.
  `SnapshotManager` keeps a running cache and applies only new commits.
  Moving *backward* (an older version than cached) requires a full replay,
  because a `remove` isn't reversible without the removed file's original
  `add` — a deliberate asymmetry, since "current state" is queried far
  more than old versions.

**`CheckpointReader`** handles compacted checkpoints (Parquet files
containing the same actions), so a table with thousands of commits doesn't
need thousands of JSON reads. This is the least-verified code in the
project — see the README.

### Layer 2 — `planner/`: which files can we skip?

This is where the actual performance win lives. Given a predicate, prune
in order of cheapness:

| Level | Uses | Opens a file? |
|---|---|---|
| Partition pruning | partition values in the log | No |
| File stats pruning | min/max/nullCount in the log's `add.stats` | No |
| Row-group pruning | Parquet footer statistics | Yes, footer only |
| Residual filter | actual decoded values | Yes, full read |

**Everything is fail-open.** If a pruner can't decide — missing stats, a
column it doesn't recognize, a type it can't compare — it *keeps* the
file. Wrongly skipping loses real data; wrongly keeping only wastes a
read. Every ambiguous case resolves toward keeping.

**`PartitionValueSubstituter`** deserves its own note. A file under
`level=ERROR/` has `level="ERROR"` for every row — but `level` doesn't
exist inside the physical Parquet file at all. So a predicate like
`level='ERROR' AND amount>2` must have its `level` clause folded to a
constant *before* reaching the decode layer, or the decoder would throw
looking for a column that isn't there. This class does that folding with
standard boolean algebra.

**Filter vocabulary:** `=`, `!=`, `<`, `<=`, `>`, `>=`, `IN`, `LIKE`,
combined with `AND`/`OR`. String comparisons are case-sensitive.
`LIKE` works correctly but doesn't prune at the file/row-group level yet
(documented gap, not a correctness issue).

### Layer 3 — `decode/`: Parquet bytes → Arrow vectors

**`ParquetToArrowReader`** is the workhorse. For each surviving file:
prune row groups via footer stats → decode only projected columns →
check each row against the residual predicate → append passing rows to
Arrow vectors.

The decode and filter are **fused into one pass** — a row that fails the
filter is never materialized into an Arrow vector at all, rather than
being decoded and then discarded.

**On "zero-copy," precisely:** Parquet is compressed and encoded on disk,
so producing Arrow buffers is real work — not literally free. What's
true: the decode goes column-chunk → Arrow array with no row-oriented
intermediate, and the *Flight transport* leg genuinely is zero-copy,
because Flight's wire format **is** Arrow IPC — no serialization step
between engine and client.

**Type support:** INT32, INT64, FLOAT, DOUBLE, BOOLEAN, BINARY (UTF-8),
INT96 (Spark's default legacy timestamp encoding — see
`Int96TimestampCodec`, including its timezone caveat), DECIMAL
(Decimal128 only, precision ≤38 — see `DecimalCodec`), and `LIST`/`MAP`/
`STRUCT` of any nesting depth via `NestedTypeDecoder` (vector embeddings,
numeric/string arrays, and arbitrary struct/map shapes), read and write.
The leaf primitive kinds a nested position bottoms out in are float,
double, int32, int64, and string — decimal/timestamp/boolean leaves
inside a nested position are not yet wired.

**`PartitionValueAppender`** reconstructs partition columns as constants,
since they exist only in folder names, not in the files.

### Layer 4 — `flight/`: serving it

**`GetFlightInfo`** does all the planning: resolve snapshot, prune, and
return **one `FlightEndpoint` per surviving file** — so a client can
fetch files in parallel across multiple streams.

**`DoGet`** does the work for exactly one file, lazily — an endpoint the
client never fetches never causes a file to be opened.

**`ListFlights`** lists catalog entries.

**Arrow Flight is gRPC**, which is why any language with a Flight client
binding works against this server unmodified — that's not a feature that
was added, it's inherent to the protocol choice.

---

## Storage: local, S3, MinIO, ADLS

Resolved in exactly one place: **`TableLocation`**. Given a string, it
decides local vs. remote by URI scheme (`s3a://`, `abfss://`, ...) and
carries the right thing downward — a `java.nio.file.Path` for local, a
Hadoop `Path` plus `Configuration` for remote.

Both the log layer (`LogStorage` → `LocalNioLogStorage` /
`HadoopLogStorage`) and the decode layer (`ParquetInputFiles`) implement
both cases, and `TableLocation` picks which.

**For MinIO specifically**, `MinioConfiguration` sets the four settings
that actually matter, one of which is easy to miss:
`fs.s3a.path.style.access=true` — AWS addresses buckets as
`bucket.s3.amazonaws.com`, MinIO as `localhost:9000/bucket`. Without it,
requests go to a hostname that doesn't exist.

---

## The catalog and CLI

**`TableCatalog`** maps a name → a location, persisted to
`~/.delta-flight-engine/catalog.json`. Small, but it matters for AI use:
handing an agent a raw filesystem path is fragile and arguably shouldn't
be something an agent needs to know.

**`delta-cli`** — subcommands, kubectl-style: `catalog add/list/remove`,
`describe`, `query`. It talks to the engine **in-process**, no server
needed, which also makes it useful for isolating "is this a server
problem or an engine problem."

---

## The RAG demo

`python/rag_demo.py` — two Ollama calls with different jobs:

1. Question + live table schema → the JSON query (retrieval)
2. Question + retrieved rows → the answer (generation)

That split is what makes it RAG rather than "an LLM near a database."

**Why it's a small script, and why that's correct:** most RAG systems are
component-heavy because unstructured text needs embedding models, chunking,
vector stores, and rerankers just to become searchable at all. Structured
data is already queryable — Flight *is* the retrieval mechanism. There's
no embedding pipeline because there's nothing unstructured to index.

---

## What this deliberately is not

- **Not a SQL engine** — one operator (scan with pushdown), no joins or
  aggregations. A real query engine could use this as its scan operator.
- **Not a writer** — no ACID transactions, by design. Spark writes; this
  reads. Implementing writes would mean re-implementing Delta's own
  commit protocol, duplicating Delta OSS rather than adding anything.
- **Not a vector database** — it can pre-filter structured metadata in
  front of one, but similarity search is a different algorithm entirely.
