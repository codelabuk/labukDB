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
| 4 | `flight/` | Serve it. `GetFlightInfo` plans and returns one `FlightEndpoint` per surviving file; `DoGet` lazily decodes exactly the file its ticket refers to. | `DeltaFlightProducer`, `DeltaFlightServerApp` |

Supporting packages: `storage/` (local/S3/MinIO/ADLS location resolution),
`catalog/` (name → table-root registry), `cli/` (the `delta-cli` tool).

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full walkthrough of each
layer, including why a table is a log and not a folder, how pruning stays
correct under ambiguity, and what "zero-copy" actually means here.

---

## Building and running

```
mvn clean package          # runs tests too; add -DskipTests to skip them
```

This produces `target/delta-cli.jar` — one file, every dependency bundled
in.

```
bin/delta-cli catalog add logs D:\repo\local-lakehouse-lab\lakehouse\logs_delta
bin/delta-cli catalog list
bin/delta-cli describe logs
bin/delta-cli query --table logs --filter "level=ERROR" --columns service,message --limit 10
```

(Windows: `bin\delta-cli.bat ...`. Zip extraction on Windows won't preserve
the executable bit on `bin/delta-cli` — run `chmod +x bin/delta-cli` once
in Git Bash, or just use `java -jar target/delta-cli.jar ...` directly.)

Every subcommand accepts either a catalog name or a raw filesystem/URI path
wherever a table is expected.

### Running the Flight server

```
java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED \
     -cp target/classes:<dependency-classpath> \
     com.labuk.deltaflight.flight.DeltaFlightServerApp
```

The `--add-opens` flag is needed because Arrow's off-heap memory library
requires it on Java 9+. `mvn test` sets this automatically via Surefire's
`argLine`; running the built jar directly needs it passed explicitly.

### Filter syntax

`=`, `!=`, `<`, `<=`, `>`, `>=`, `IN`, `LIKE` (SQL-style, `%`/`_`,
case-sensitive), combined with `AND`/`OR`. No generic `NOT` — see
`Predicate`'s javadoc for why that doesn't lose expressiveness.

---

## The catalog

`TableCatalog` maps a short name to a table location, persisted to
`~/.delta-flight-engine/catalog.json` — so a client (especially an AI
agent or MCP tool) can say `"logs"` instead of needing to know an exact
path. Not Unity Catalog, and doesn't try to be.

## Storage backends

Local disk, S3/MinIO (`s3a://`, via optional `hadoop-aws`), and ADLS Gen2
(`abfss://`, via optional `hadoop-azure`) are all resolved through one
entry point, `TableLocation`, by URI scheme. Both dependencies are
optional — a local-only consumer isn't forced to pull in either SDK.

Currently: the log/metadata layer is fully storage-abstracted; the
Parquet *data* read path in `decode/` still assumes local files, so a
genuine end-to-end remote query isn't wired through yet.

## Python client & RAG demo

- `python/query_demo.py` — a plain `pyarrow.flight` client against a
  running `DeltaFlightServerApp`. Proof that any Flight-capable client in
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
