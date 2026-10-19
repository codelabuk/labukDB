# V2 — Lakehouse Gateway: architecture plan

Captures the direction agreed on for v2: generalizing this project from a
Delta-only read engine into a multi-format lakehouse gateway (Delta +
Iceberg), while staying in Java for now. This is a plan, not yet
implemented — treat it as the design to work from for the next round of
changes, not a status report.

See [ARCHITECTURE.md](ARCHITECTURE.md) for how the current (Delta-only)
engine works today; this document only covers what changes for v2.

---

## Why this direction

- The existing four-layer design (`log` → `planner` → `decode` → `flight`)
  already matches most of InfluxDB 3.0's architecture (Parquet in object
  storage, Arrow in memory, Flight for serving, pushdown/pruning) — see
  the "InfluxDB 3.0 Architecture" diagram from *Building InfluxDB 3.0 with
  Apache Arrow, DataFusion, Flight and Parquet*. The one box this project
  doesn't have is the DataFusion-equivalent query execution engine.
- Apache DataFusion has no official Java bindings, so it can't be dropped
  into this codebase directly. A Rust rewrite using DataFusion is a real
  future option, but a separate project — tracked as "learn Rust, then
  revisit this with DataFusion," not a blocker for v2.
- Performance concern (Java vs. Rust) is real but narrow: I/O, pruning,
  Parquet→Arrow decode, and Flight transport are not meaningfully
  language-bound (Arrow buffers are already off-heap; object-store latency
  dominates). The one place Java genuinely loses to hand-tuned native code
  is CPU-bound vectorized compute — aggregation, expression evaluation,
  joins. **Apache Arrow Gandiva** (LLVM-JIT expression compiler, official
  Java bindings via `arrow-gandiva`) is the planned mitigation if/when
  naive Java aggregation proves to be an actual bottleneck — not applied
  preemptively.

## Target architecture

```
Client (SQL/Flight) ───▶  flight/        (serving — mostly unchanged)
                                │
                           execution/     (NEW — the DataFusion-shaped gap)
                           filter · project · aggregate (GROUP BY,
                           SUM/COUNT/MIN/MAX/AVG) · sort/limit
                                │
                           planner/       (unchanged in spirit — needs
                           partition/file/row-group pruning, fail-open      AddFile generalized to
                                │                                           a format-neutral DataFileRef)
                           decode/        (unchanged — one Parquet→Arrow
                                │           decoder serves both formats)
                     ┌──────────┴──────────┐
                     │       format/ (NEW)   │
                     │ TableFormat → unified │
                     │ { schema, partitionSpec,
                     │   List<DataFileRef> } │
              ┌───────┴───────┐      ┌───────┴────────┐
              │ DeltaTableFormat│      │ IcebergTableFormat│
              │ (wraps existing │      │ (wraps iceberg-core,│
              │  log/ package)  │      │  not hand-rolled)   │
              └─────────────────┘      └─────────────────────┘
                                │
                           storage/       (unchanged — local/S3/MinIO/ADLS)
```

### What's reused untouched

- `decode/` — Parquet → Arrow decode, row-group pruning, residual
  filtering. Format-agnostic already; both Delta and Iceberg tables are
  commonly backed by Parquet data files.
- `storage/` — `TableLocation` + local/S3/MinIO/ADLS resolution.
- `log/` (Delta-specific commit/snapshot handling) — becomes the
  implementation behind `DeltaTableFormat`, not replaced.

### What's new

| Component | Scope | Notes |
|---|---|---|
| `format/TableFormat` | Small, mechanical | Interface normalizing Delta and Iceberg snapshots to one shape: schema, partition spec, `List<DataFileRef>` |
| `format/DataFileRef` | Small | Generalization of today's `AddFile` (path, partition values, size, stats) so `planner/` doesn't care which format produced it |
| `format/IcebergTableFormat` | Medium | **Decision: use `iceberg-core`** (the official Apache Iceberg Java library) for manifest-list/manifest-file parsing, rather than hand-rolling Avro parsing the way `log/` hand-rolled Delta's JSON log. Iceberg's metadata surface (manifest lists, manifest files, partition spec evolution, hidden partitioning) is materially bigger than Delta's, and the "build it from scratch to understand it" value of a second from-scratch log-format implementation is low now that the goal has shifted from a learning exercise to an efficient product. |
| `execution/` | The real new subsystem | Start minimal: `GROUP BY` + `SUM/COUNT/MIN/MAX/AVG`, `ORDER BY ... LIMIT`. Covers most dashboard/time-series query shapes without building a general optimizer. Evaluate Gandiva only once plain Java aggregation is shown to be a bottleneck. |
| Catalog `format` field | Small | `TableCatalog` entries gain `delta` \| `iceberg` so one catalog can dispatch by format — already named as a roadmap item ("multi-format dispatcher") before this plan existed. |

## Sequencing (suggested, not committed)

1. `format/TableFormat` + `DataFileRef` — refactor `planner/` to consume
   the new abstraction instead of `AddFile` directly. Pure refactor,
   `DeltaTableFormat` should make existing tests pass unchanged.
2. `IcebergTableFormat` via `iceberg-core` — first non-Delta table
   readable end-to-end through the same `planner`/`decode`/`flight` path.
3. `execution/` — minimal aggregation (`GROUP BY` + a few aggregate
   functions) and `ORDER BY ... LIMIT`, wired into `flight/` as a new
   query shape alongside the existing plain scan.
4. Catalog `format` field + CLI/`TableResolver` dispatch.
5. Revisit Gandiva only if a real workload shows Java aggregation is the
   bottleneck, not before.

## Explicitly deferred

- A Rust/DataFusion rewrite — real option once Rust is further along, but
  a separate project reusing these architecture decisions, not a
  continuation of this codebase.
- Joins, window functions, a general query optimizer — out of scope until
  a concrete requirement needs them; matches this project's existing
  "one operator, add complexity only when justified" stance.
