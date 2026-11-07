# labukDB — SQL Engine Architecture (`core.logical`)

```mermaid
flowchart TB
    classDef builtNode fill:#EEEDFE,stroke:#534AB7,color:#26215C
    classDef reusedNode fill:#F1EFE8,stroke:#888780,color:#2C2C2A
    classDef decisionNode fill:#FFF1CF,stroke:#888780,color:#2C2C2A

    SQTR["SqlQueryTranslator<br/>(jsqlparser-based — a real SQL<br/>grammar, not hand-rolled)"]:::reusedNode
    SQQ["SqlQuery<br/>(table, columns, predicate, groupBy,<br/>aggregates, orderBy, limit, join)"]:::reusedNode

    LPB["LogicalPlanBuilder.build()"]:::builtNode
    RBO["RuleBasedOptimizer.DEFAULT<br/>.optimize()"]:::builtNode
    LPE["LogicalPlanExecutor.resolve() /<br/>.resolveJoin(format, dataFileRoot,<br/>version, asOf)"]:::builtNode
    RS["ResolvedScan / JoinExecution<br/>(files, columns, aggregate,<br/>sort, limit, snapshot metadata)"]:::builtNode

    SQTR -- "translate(sql)" --> SQQ
    SQQ --> LPB
    LPB --> RBO
    RBO --> LPE
    LPE --> RS

    QC["QueryCommand.runSql /<br/>.runSqlJoin"]:::reusedNode
    DECISION["needsCombine()?<br/>(aggregate/sort/limit present)"]:::decisionNode
    CQE["CombinedQueryExecutor.execute(...)<br/>/ HashJoinOperator.innerJoin(...)"]:::reusedNode
    STREAM["scanAndPrintAllFiles(...)<br/>(unified per-file streaming,<br/>both formats)"]:::builtNode
    OUT1["printHeader + printRows<br/>+ exact row count"]:::reusedNode
    OUT2["printHeader + printRows<br/>per file, no cap"]:::builtNode

    RS --> QC
    QC --> DECISION
    DECISION -- "true" --> CQE
    DECISION -- "false" --> STREAM
    CQE --> OUT1
    STREAM --> OUT2
```

**Tree shape before optimization** (`LogicalPlanBuilder`'s output — one `Project` always wraps the whole thing, and `Join` adds a second `TableScan` branch when the query joins two tables):

```mermaid
flowchart TB
    B1["Project(columns)"] --> B2["Limit(n)"]
    B2 --> B3["Sort(sortKeys)"]
    B3 --> B4["Aggregate(groupBy, aggregates)"]
    B4 --> B5J["Join(keys)"]
    B5J --> B5L["Filter(leftPredicate)"]
    B5J --> B5R["Filter(rightPredicate)"]
    B5L --> B6L["TableScan(leftTable, null, null)"]
    B5R --> B6R["TableScan(rightTable, null, null)"]
```

**Tree shape after optimization** (`PushFilterIntoScan` absorbs each `Filter` directly onto its own `TableScan`; `PushProjectIntoScan` absorbs and drops the outer `Project` — *unless* a `Join` is present with no `Aggregate` above it, in which case `Project` survives to trim the join's wider combined columns back down to the real `SELECT` list):

```mermaid
flowchart TB
    A1["Limit(n)"] --> A2["Sort(sortKeys)"]
    A2 --> A3["Aggregate(groupBy, aggregates)"]
    A3 --> A4J["Join(keys)"]
    A4J --> A5L["TableScan(leftTable, leftPredicate, null)"]
    A4J --> A5R["TableScan(rightTable, rightPredicate, null)"]
```

Only nodes the query actually used survive — e.g. a plain `SELECT x FROM t` optimizes straight down to one bare `TableScan`, nothing wrapping it at all. `requestedColumns` is the exact union `QueryCommand` used to compute inline before this rule existed: group-by + aggregate-input columns when aggregating, else `SELECT` list + `ORDER BY` columns. `WHERE`-only columns need no explicit handling — `ParquetToArrowReader` already unions predicate columns in at decode time. For a join, column pruning is deliberately *not* pushed across the join boundary — each side's `TableScan` keeps `requestedColumns == null` ("decode everything"), a stated simplification.

**`LogicalPlanExecutor` deliberately does NOT open the table itself.** Format detection (Delta vs. Iceberg vs. REST-catalog Iceberg) needs cross-module visibility into `labukdb-format-iceberg` that `labukdb-core` doesn't have. `QueryCommand` (in `labukdb-cli`, which depends on both modules) resolves the table name into an already-open `TableFormat` + the `TableLocation` its data files resolve against, *first*, then hands both in here. This class's job starts at snapshot resolution (`format.latest()`/`atVersion()`/`atTimestamp()`) and ends at "files to scan" — the genuinely format-agnostic part. For a join, each side is resolved completely independently via the same `resolveSide` helper, which is what makes a Delta↔Iceberg join work with zero format-specific join code — a free consequence of the design, not a built feature.

Real gap found and fixed to make this true: `DeltaTableFormat` existed but only had a local-only `Path` constructor — would have silently broken MinIO Delta SQL queries if wired in naively. Added `DeltaTableFormat(TableLocation)`, delegating to the already-remote-capable `SnapshotManager.forLocation`.

**Deliberate, named behavioral improvement, not a silent regression**: Iceberg's CLI path previously *always* went through `CombinedQueryExecutor` regardless of whether the query aggregated/sorted/limited. Now streams per-file for a plain `SELECT` on either format, matching Delta's existing smarter behavior — lower memory, identical output.

## Scope boundaries, explicit

- Flight (`LabukDbFlightProducer`) untouched — its own JSON query protocol is a separate, pre-decomposed shape, never SQL text. Mapping it onto this same `LogicalPlan` is natural future work, not done here.
- `Predicate`'s leaves are still only `column OP literal` or (for a join's `ON`/key columns) `column OP column` within an equi-join — no generic cross-column comparison outside that, no `NOT`.
- Still not supported: `LEFT`/`RIGHT`/`FULL` joins, non-equi join conditions, 3+-way joins, cost-based build/probe side selection, subqueries, computed `SELECT` expressions (every item must be a bare column or one of the 5 aggregate functions).

## The arc, where it actually stands

Phase 1 (logical plan IR + optimizer shell) — **done**. Phase 2 (`LIMIT` early-termination without `ORDER BY`/`GROUP BY`) — **done**. Phase 3, increment 1 (two-table `INNER` equi-join) — **done**, live-verified against real MinIO, including a Delta↔Iceberg mixed join. Phase 3, increment 2 (`GROUP BY`/aggregates/`ORDER BY`/`LIMIT` combined with a `JOIN`) — **done**, live-verified against real MinIO. Phase 3's remaining piece (`LEFT`/`RIGHT`/`FULL` joins, 3+-way joins, cost-based join-side selection) — not started. Phase 4 (subqueries: scalar, `IN (SELECT...)`, `EXISTS`) — not started.
