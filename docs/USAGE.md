# Using labukDB

Four ways to talk to the same engine — pick based on whether you need a
server, want real SQL, or are embedding the reader/writer directly inside
your own JVM process. All four ultimately run the same `log/` →
`planner/` → `decode/`/`execution/` pipeline; none of them is a "thin"
or "limited" version of another.

| Mode | Needs a server? | Read | Write | Best for |
|---|---|---|---|---|
| [CLI, flags](#1-cli-flag-based) | No | ✅ | ❌ (query-only) | Quick terminal lookups, scripting, debugging |
| [CLI, SQL](#2-cli-sql) | No | ✅ | ❌ (query-only) | Real `SELECT` text, `GROUP BY`/`ORDER BY`/`LIMIT` from a terminal |
| [Flight server](#3-flight-server-remote-clients) | Yes | ✅ | ✅ | Remote clients, any language, multiple concurrent readers/writers |
| [Embedded library](#4-embedded-library-labukdb-core-directly) | No | ✅ | ✅ | Embedding inside your own JVM app, no IPC/network hop (DuckDB-style) |

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for how the layers
underneath these four modes actually work, and what's built vs. still
planned.

### Delta vs. Iceberg

Modes 1–3 (CLI flags, CLI SQL, Flight) all auto-detect which table format
a location holds — probing for a `_delta_log/` directory (Delta) or a
`metadata/` directory (Iceberg), or recognizing an `icebergrest://`
reference before any path-probing happens at all (see
[Iceberg via a REST catalog](#iceberg-via-a-rest-catalog) below) — so the
same `--table`/`FROM`/`FlightQueryRequest` works against any of them
without saying which one it is. A few real differences to know about:

- **Iceberg writes are append-only, and not reachable from the CLI.**
  `DoPut` (Flight) and `IcebergTableWriter` (embedded library) can append
  to an already-existing Iceberg table — no CREATE TABLE, no merge/
  upsert, no compaction, no vacuum, and the CLI still has no write
  subcommands at all for *either* format, so this isn't CLI-reachable.
- **`--version`/`version` means something different per format.** For
  Delta it's a small sequential commit version; for Iceberg it's a real,
  non-sequential `snapshotId` (a large pseudo-random long) — see
  `IcebergTableFormat`'s javadoc. Pointing `--version 5` at an Iceberg
  table almost certainly won't mean what it means for Delta.
- **Iceberg queries always run in combine mode.** Even a plain `SELECT`
  with no `GROUP BY`/`ORDER BY`/`LIMIT` decodes and combines every
  surviving file through the same executor combine-mode queries use,
  rather than streaming per file the way Delta's plain scans do — a
  deliberate v1 simplification so Iceberg doesn't need its own parallel
  per-file-streaming implementation. Correctness isn't affected; very
  large Iceberg result sets just won't stream incrementally yet.
- **File-stats pruning doesn't apply to Iceberg files yet** (only
  partition-value pruning does) — Iceberg's own column-level stats
  aren't mapped into the shape the pruner reads today. Files are kept
  rather than wrongly skipped, so this is a missed optimization, not a
  correctness gap.

### Iceberg via a REST catalog

Most real Iceberg deployments register tables in a catalog *service*
(REST, Hive Metastore, Glue, Nessie) rather than addressing them by a raw
filesystem path — `HadoopTables` (everything above) only covers the
path-based minority case. A REST catalog is reachable too, in the same
three modes (CLI flags, CLI SQL, Flight), using an `icebergrest://`
reference instead of a path:

```
bin/labukdb query --table icebergrest://analytics.logs --filter "level=ERROR"
bin/labukdb query --sql "SELECT level, COUNT(amount) AS cnt FROM icebergrest://analytics.logs GROUP BY level"
```

The identifier after `icebergrest://` is the table's catalog identifier
(`<namespace>.<table>`, dotted — multi-level namespaces work too, e.g.
`icebergrest://prod.analytics.logs`), **not** a filesystem path. It can
also be registered under a short catalog name the normal way
(`bin/labukdb catalog add logs icebergrest://analytics.logs`).

**The catalog *connection* is separate from the table reference**,
configured once from the environment — deliberately not packed into the
URI itself, the same separation `MinioConfiguration`/`AdlsConfiguration`
already use for storage connectors:

| Variable | Required? | Meaning |
|---|---|---|
| `ICEBERG_REST_URI` | Yes | The REST catalog's base URL |
| `ICEBERG_REST_WAREHOUSE` | No | Warehouse location, if the catalog needs one |
| `ICEBERG_REST_AUTH_TYPE` | No | `none` (default) / `basic` / `oauth2` / `sigv4` / `google` |
| `ICEBERG_REST_TOKEN`, `ICEBERG_REST_CREDENTIAL`, `ICEBERG_REST_OAUTH2_SERVER_URI` | For `oauth2` | OAuth2 bearer token / client credential / token-endpoint override |
| `ICEBERG_REST_BASIC_USERNAME`, `ICEBERG_REST_BASIC_PASSWORD` | For `basic` | HTTP Basic auth |

**v1 limitation, stated plainly: one REST catalog connection per
process.** Not a registry of multiple catalogs — same scope the existing
MinIO/ADLS env-var config already has.

**The catalog only resolves *which table*; its underlying storage still
needs its own connector config too.** If the REST catalog's tables live
on S3/MinIO/ADLS, set `MINIO_*`/`ADLS_*` env vars the normal way *in
addition to* `ICEBERG_REST_*` — the REST catalog hands back a storage
location, and reading/writing that location still goes through this
project's existing `TableLocation`-based connectors, same as everywhere
else.

**Live-verified against a real REST catalog server** —
`tabulario/iceberg-rest` (the standard open-source reference
implementation), run locally: connect, create a namespace/table, load it
back with the correct schema and partition spec, drop it — a genuine HTTP
round trip, not just an in-process `Catalog` call. Scoped to metadata/
wire-protocol operations, not yet a full write-then-read data round trip
(aligning `IcebergTableWriter`'s own JVM-side data writes with a REST
server's warehouse path across host/container boundaries is its own
separate constraint). The format-agnostic `Catalog`-based logic this feeds
into (`IcebergTableFormat`/`IcebergTableWriter`'s `Catalog` constructors)
is also fully tested against a real `InMemoryCatalog` (a genuine Iceberg
`Catalog` implementation, not a mock) — proving the actual read/write
logic is correct for *any* catalog, `RESTCatalog` included, since they
share one interface.

---

## Building

```
mvn clean package          # runs tests too; add -DskipTests to skip them
```

Produces `labukdb-cli/target/labukdb-cli.jar` (one file, every dependency
bundled) and `labukdb-server-app/target/labukdb-server-app.jar`, the
standalone Flight server. (Windows: `bin\labukdb.bat ...`; on Git Bash,
`chmod +x bin/labukdb` once, or just run the jar directly — `describe`/
`query` need the same `--add-opens` flag the wrapper scripts already
bake in: `java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
-jar labukdb-cli/target/labukdb-cli.jar ...`.)

Every subcommand below, and every `--table`/`FROM`, accepts **either** a
name registered via `catalog add` **or** a raw filesystem/URI path —
there's no separate "resolve a name" step to remember.

---

## 1. CLI, flag-based

```
bin/labukdb catalog add logs D:\repo\local-lakehouse-lab\lakehouse\logs_delta
bin/labukdb catalog list
bin/labukdb catalog remove logs
bin/labukdb describe logs
bin/labukdb query --table logs --filter "level=ERROR" --columns service,message --limit 10
```

`describe` prints the schema, partition columns, active file count, and
current version — no row data touched.

`query` options:

| Flag | Meaning |
|---|---|
| `--table <name-or-path>` | required (unless `--sql` is used instead, see below) |
| `--columns col1,col2` | project only these columns (default: all) |
| `--filter "expr"` | see filter syntax below (`AND`-only) |
| `--version N` | query as of a specific commit version |
| `--as-of TIMESTAMP` | query as of an ISO-8601 instant, e.g. `2026-07-17T00:00:00Z` (mutually exclusive with `--version`) |
| `--limit N` | cap printed rows, `0` = unlimited (default 50) |

**Filter syntax** (`SimpleFilterParser`) — deliberately small, not SQL:

```
level=ERROR
amount>2
level='ERROR' AND amount>2        # AND only -- no OR, no parentheses
level IN (ERROR,WARN)
log_date LIKE '2026-07%'
```

`=`, `!=`, `<`, `<=`, `>`, `>=`, `IN`, `LIKE` (SQL-style `%`/`_`,
case-sensitive). Values are inferred as Long, then Double, then String,
unless wrapped in single quotes to force a string (useful when a value
looks numeric, e.g. `region='001'`). No generic `NOT`. For `OR`,
parentheses, or anything past this, use `--sql` instead (section 2) — the
real wire format behind both (`Predicate`/`PredicateCodec`) already
supports `OR` and nesting; this parser just doesn't expose it at the
terminal.

---

## 2. CLI, SQL

A real `SELECT` statement, parsed by `SqlQueryTranslator` (built on
`jsqlparser`) straight into the same `Predicate`/`AggregateSpec`/
`SortKey` structures the engine already executes correctly — not a
separate, weaker code path.

```
bin/labukdb query --sql "SELECT level, COUNT(amount) AS cnt FROM logs GROUP BY level"
bin/labukdb query --sql "SELECT amount FROM logs ORDER BY amount DESC LIMIT 3"
bin/labukdb query --sql "SELECT service, amount FROM logs WHERE level = 'ERROR' AND amount > 2"
```

`--sql` is **mutually exclusive** with `--table`/`--columns`/`--filter`/
`--limit` — the SQL text's own `FROM`/`SELECT`/`WHERE`/`LIMIT` clauses
subsume all four. `--version`/`--as-of` still combine with `--sql`
normally (time travel is orthogonal to query text):

```
bin/labukdb query --sql "SELECT * FROM logs" --as-of 2026-07-17T00:00:00Z
```

**Supported subset:**

```
SELECT <col> | <AGGFUNC>(<col>) AS <alias> [, ...]
FROM <table>
[[INNER|LEFT|RIGHT|FULL] JOIN <table> ON <col> = <col> [AND ...]] [, ...]
[WHERE <predicate>]
[GROUP BY <col> [, ...]]
[ORDER BY <col> [ASC|DESC] [, ...]]
[LIMIT <n>]
```

- Predicate: `=`, `!=`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `IN (...)`,
  `LIKE` — exactly `Predicate`'s own capability.
- Aggregate functions: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` — an aggregate
  `SELECT` item **requires an alias** (`COUNT(amount) AS cnt`, not bare
  `COUNT(amount)`).
- A plain `SELECT`+`WHERE` (no `GROUP BY`/aggregates/`ORDER BY`/`LIMIT`)
  streams results per-file as it reads them. Anything needing `GROUP BY`/
  `ORDER BY`/`LIMIT` is answered by `CombinedQueryExecutor` — every
  surviving file is decoded and combined first, because those can't be
  answered correctly file-by-file (a per-file top-N or per-file count
  would be silently wrong). Unlike the flag-based path, a bare SQL
  `SELECT` with no `LIMIT` clause prints **every** matching row — that's
  what "no `LIMIT`" means in SQL, so it isn't silently capped at 50.
- `JOIN` — any number of tables, `INNER`/`LEFT`/`RIGHT`/`FULL`, equi-join
  only (`ON` must be `AND`-combined column-to-column equalities). Each
  table may be a different format — a Delta table can join an Iceberg
  table with no format-specific code. Every column anywhere in the query
  must be table-qualified (`a.service`, not `service`) once a `JOIN` is
  present. `GROUP BY`/aggregates/`ORDER BY`/`LIMIT` all combine with a
  `JOIN` normally, applied to the joined result.
- `IN (SELECT ...)`, `NOT IN (SELECT ...)`, `EXISTS (SELECT ...)`,
  `NOT EXISTS (SELECT ...)` — decorrelated internally into a join (same
  "every column must be table-qualified" rule as above kicks in once one
  of these is present, even with no explicit `JOIN` keyword at all). The
  subquery itself must be a single plain table (no `JOIN`/`GROUP BY`/
  `ORDER BY`/`LIMIT` inside it, no further nested subquery), and
  correlation to an outer table must be a plain `AND`-combined
  `inner.col = outer.col` equality. A wholly uncorrelated `EXISTS`/
  `NOT EXISTS` (no shared column with any outer table at all) isn't
  supported yet. `NOT IN (SELECT ...)` doesn't replicate standard SQL's
  NULL-poisoning rule for a nullable subquery result column — a stated,
  known gap, not silently assumed correct.
- `WHERE x OP (SELECT ...)` (a scalar subquery used as a comparison
  operand) — runs once, before the rest of the query, substituting its
  single value. The inner query can be anything a top-level query can
  (including a group-by-less aggregate, e.g. `(SELECT MAX(y) FROM b)`
  — the most common real shape) except its own `JOIN` or `GROUP BY`.
  Correlation to the outer query isn't supported (rejected as a side
  effect of translating the inner query independently). A subquery
  returning zero rows, or whose one value is `NULL`, makes the whole
  query return zero rows; more than one row is a runtime error.

**Rejected with a specific error, not silently mishandled:**

| Construct | Why |
|---|---|
| `NOT`, `NOT LIKE`, `NOT IN (<literal list>)` | `Predicate` has no generic negation — see its javadoc. `NOT IN (SELECT ...)` *is* supported (see above); only the literal-list form is rejected. |
| `IS NULL` | not a supported predicate shape |
| A scalar subquery with its own `JOIN` or `GROUP BY` inside it | not implemented yet |
| A group-by-less aggregate scalar subquery over zero matching files | would need a real `NULL`/zero aggregate result, which can't be produced without a file to read a schema from |
| A scalar subquery nested inside an `IN`/`EXISTS` subquery's own filter | that filter uses a narrower translation path that doesn't share this detection pass |
| A derived table in `FROM` (`FROM (SELECT ...) AS t`) | not implemented yet |
| `CROSS`/`NATURAL` join, non-equi `ON` conditions | not implemented yet |
| A `WHERE` condition on the non-preserved side of `LEFT`/`RIGHT`, or on either side of `FULL` | pre-filtering it before the join would silently change which rows get null-padded — a condition on the preserved side is fine |
| `COUNT(*)` | matches `AggregateOperator`'s pre-existing `COUNT(column)`-only semantics (non-null values of a named column) |
| `IN` combined with `AND`/`OR` **without parentheses** | see below |

**`IN` must be parenthesized when combined with `AND`/`OR`.** Confirmed
against a real parse: `jsqlparser`'s grammar lets a bare `col IN (...)`
greedily consume a trailing `OR`/`AND` as part of its own right-hand
expression instead of binding tighter than `OR`, the way standard SQL
precedence would. In practice:

```sql
-- Misparses -- rejected with an error naming the fix, not silently wrong:
SELECT service FROM logs WHERE level IN ('ERROR','WARN') OR service LIKE 'auth%'

-- Correct -- wrap the IN predicate:
SELECT service FROM logs WHERE (level IN ('ERROR','WARN')) OR (service LIKE 'auth%')
```

See `SqlQueryTranslator`'s javadoc for the full, exact subset and the
diagnosis behind this.

---

## 3. Flight server (remote clients)

```
java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED \
     -jar labukdb-server-app/target/labukdb-server-app.jar
```

The `--add-opens` flag is required because Arrow's off-heap memory
library needs it on Java 9+ (`mvn test` sets it automatically via
Surefire's `argLine`; running the built jar directly needs it explicit).

A client sends a JSON-encoded request as a `FlightDescriptor`'s command
bytes. **Reads** go through `GetFlightInfo`/`DoGet`; **writes** through
`DoPut`.

### Reading — `GetFlightInfo` / `DoGet`

Full `FlightQueryRequest` field reference:

| Field | Meaning |
|---|---|
| `table` | a name registered in `TableCatalog` — exactly one of `table`/`tableRoot` required |
| `tableRoot` | a raw filesystem/URI path (the original, still-supported form) |
| `version` | query as of a specific commit version (mutually exclusive with `timestampMillis`) |
| `timestampMillis` | query as of a specific instant |
| `columns` | project only these columns; null/empty = all columns |
| `predicate` | see predicate shape below; null = no filter |
| `groupBy` | list of column names |
| `aggregates` | list of `{"function", "column", "outputName"}` — `function` is `SUM`/`COUNT`/`MIN`/`MAX`/`AVG` |
| `orderBy` | list of `{"column", "ascending"}` (`ascending` defaults to `true`) |
| `limit` | row cap after any `GROUP BY`/`ORDER BY` |

Predicate shape (`PredicateCodec`) — a small recursive tree:

| `type` | Required fields |
|---|---|
| `cmp` | `column`, `op` (`EQ`/`NEQ`/`GT`/`GTE`/`LT`/`LTE`), `literal`, `literalType` |
| `and` / `or` | `children`: array of nested predicate nodes |
| `in` | `column`, `literals`: array, `literalType` |
| `like` | `column`, `pattern` (SQL-style `%`/`_`, case-sensitive) |

`literalType` is one of `string`, `long`, `double`, `boolean`,
`timestamp` (ISO-8601, e.g. `"2026-07-17T10:00:00Z"`).

**Plain scan** — one `FlightEndpoint` per surviving physical file,
decoded lazily per ticket:

```json
{
  "table": "logs",
  "columns": ["level", "message", "event_ts"],
  "predicate": {
    "type": "cmp", "column": "level", "op": "EQ",
    "literal": "ERROR", "literalType": "string"
  }
}
```

**`GROUP BY`/`ORDER BY`/`LIMIT`** — any of `groupBy`/`aggregates`/
`orderBy`/`limit` present switches `GetFlightInfo` onto a **single
combined endpoint** instead of one-per-file: every surviving file is
decoded, combined, then run through `AggregateOperator`/
`SortLimitOperator` server-side before streaming back one correct,
cross-file result (a per-file-only answer would be silently wrong for a
global top-N or a cross-partition count):

```json
{
  "table": "logs",
  "groupBy": ["level"],
  "aggregates": [
    {"function": "COUNT", "column": "amount", "outputName": "cnt"},
    {"function": "SUM", "column": "amount", "outputName": "total"}
  ],
  "orderBy": [{"column": "total", "ascending": false}],
  "limit": 10
}
```

**No SQL-over-Flight yet** — a client builds this JSON shape directly
today, the same way the CLI's flag-based path does under the hood;
translating SQL text into this shape server-side is a deliberate,
separate follow-up (the `--sql` CLI flag from section 2 is CLI-only for
now).

### Writing — `DoPut`

The table must already exist (`DoPut` only appends — creating a table is
`TableCreator`, a library-only capability not yet exposed over Flight).
The command bytes carry a `FlightWriteRequest`:

```json
{
  "table": "logs",
  "partitionValues": {"level": "ERROR"}
}
```

Exactly one of `table`/`tableRoot` is required, same rule as reads.
`partitionValues` is required — `WriteBuffer` buffers one table **and**
one fixed partition combination at a time, so a single `DoPut` call
always targets exactly one partition. Stream Arrow batches matching the
table's schema (minus partition columns) after the descriptor; the
server buffers them (WAL-backed) and flushes to a committed Parquet file
+ log entry once a size/time threshold is hit.

**`DoPut` against an Iceberg table** works the same way from the
client's side (same JSON request shape, same streamed-batches protocol),
but is handled differently server-side: no `WriteBuffer`, no WAL — every
streamed batch in the call is combined into one Arrow batch and committed
as exactly **one new Iceberg snapshot** per `DoPut` call (via the new
`IcebergTableWriter`, reusing `iceberg-core`'s own `Table.newAppend()`,
which already retries internally under a concurrent-writer conflict — no
hand-built OCC loop needed here, unlike Delta's). The table must already
exist; there's still no Iceberg CREATE TABLE support. `tableRoot`/`table`
works as an `icebergrest://` reference too, same as reads — see
[Iceberg via a REST catalog](#iceberg-via-a-rest-catalog) above.

### From Python (or any Flight-capable client, any language)

```python
import json
import pyarrow.flight as flight

client = flight.FlightClient("grpc://localhost:9443")
query = {
    "tableRoot": r"D:\repo\local-lakehouse-lab\lakehouse\logs_delta",
    "predicate": {"type": "cmp", "column": "level", "op": "EQ",
                  "literal": "ERROR", "literalType": "string"},
}
descriptor = flight.FlightDescriptor.for_command(json.dumps(query).encode("utf-8"))

info = client.get_flight_info(descriptor)
for endpoint in info.endpoints:
    table = client.do_get(endpoint.ticket).read_all()
    print(table)
```

This is proof any Flight client in any language works against this
server unmodified, since Flight *is* gRPC — the example above is the
whole client, not a trimmed excerpt of a larger one.

---

## 4. Embedded library (`labukdb-core` directly)

No CLI, no server — `labukdb-core` has no Flight/gRPC dependency, so this
is the DuckDB-style embedded path: link it into your own JVM process and
call the engine in-process.

Not published to Maven Central — build and install it locally first:

```
mvn clean install -DskipTests
```

```xml
<dependency>
    <groupId>com.labuk</groupId>
    <artifactId>labukdb-core</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

### Reading

The four layers map onto four calls — resolve a snapshot, plan which
files survive pruning, fold each file's partition values into the
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

SnapshotManager snapshotManager = new SnapshotManager(tableRoot);
Snapshot snapshot = snapshotManager.latest();          // or atVersion(n) / atTimestamp(instant)

Predicate filter = Predicate.compare("level", ComparisonOp.EQ, "ERROR");
ScanPlan plan = new QueryPlanner().plan(snapshot, filter, List.of("service", "message"));

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

Need `GROUP BY`/`ORDER BY`/`LIMIT` too, same as the CLI's `--sql` path
and Flight's combine mode? Build a `List<FileScan>` from `plan.getFilesToScan()`
(mirrors the loop above) and call `CombinedQueryExecutor.execute(...)` —
or just reuse `SqlQueryTranslator.translate(sql)` first to get the
`Predicate`/`AggregateSpec`/`SortKey` structures from real SQL text
instead of building them by hand. See `QueryCommand` (`labukdb-cli`) for
the complete wiring of both paths.

`SnapshotManager` and `ParquetToArrowReader` also have `TableLocation`-based
overloads (`SnapshotManager.forLocation(...)`) for S3/MinIO/ADLS tables —
see [Configuring a remote backend](#configuring-a-remote-backend) below,
and `labukdb-core/src/main/java/com/labuk/labukdb/core/tools/RealTableSmokeCheck.java`
for a second, runnable end-to-end example.

**Reading an Iceberg table** looks the same, swapping `SnapshotManager`/
`QueryPlanner.plan`/`ScanPlan`/`AddFile` for `IcebergTableFormat`/
`QueryPlanner.planNormalized`/`NormalizedScanPlan`/`DataFileRef` — the
`labukdb-format-iceberg` module, a separate dependency (`iceberg-core`
isn't pulled in unless you ask for it):

```java
import com.labuk.labukdb.core.format.DataFileRef;
import com.labuk.labukdb.core.format.NormalizedSnapshot;
import com.labuk.labukdb.core.planner.NormalizedScanPlan;
import com.labuk.labukdb.iceberg.IcebergTableFormat;
import org.apache.hadoop.conf.Configuration;

IcebergTableFormat tableFormat = new IcebergTableFormat(tableRoot.toString(), new Configuration());
NormalizedSnapshot snapshot = tableFormat.latest();   // or atVersion(snapshotId) / atTimestamp(instant)

Predicate filter = Predicate.compare("level", ComparisonOp.EQ, "ERROR");
NormalizedScanPlan plan = new QueryPlanner().planNormalized(snapshot, filter, List.of("service", "message"));

for (DataFileRef file : plan.getFilesToScan()) {
    Predicate residual = PartitionValueSubstituter.simplify(
            plan.getResidualPredicate(), file.getPartitionValues());
    // same ParquetToArrowReader.read(...) call as the Delta example above
}
```

`IcebergTableFormat` also has a `TableLocation`-based constructor
(`new IcebergTableFormat(location)`) for the same local/remote dispatch
`DeltaTableFormat` and everything else use, **and** a catalog-based one —
`new IcebergTableFormat(catalog, identifier)`, taking any real Iceberg
`Catalog` (`RESTCatalog`, `HadoopCatalog`, your own) and a
`TableIdentifier` — for addressing a table by catalog registration
instead of a path; see
[Iceberg via a REST catalog](#iceberg-via-a-rest-catalog) above for the
CLI/Flight-level `icebergrest://` convenience built on top of this same
constructor. See [Delta vs. Iceberg](#delta-vs-iceberg) above for the
real differences (append-only writes, `atVersion`'s `snapshotId`
semantics, no file-stats pruning yet) — all apply here too, not just to
the CLI/Flight paths.

### Writing

`TableCreator` and `CommitWriter` both take a `TableLocation`, so this
works identically against a local path or a remote URI:

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

`TableMerger.merge(...)` does insert/update/delete by key on top of this
(three separate batches, key supplied per call — not an
operation-indicator column, not stored table metadata; see its javadoc
for the exact semantics). `Compactor` (bin-packing) and `Vacuum`
(orphaned-file removal) both have matching `TableLocation` overloads too
— all four local and remote, live-MinIO-verified.

**Writing to an Iceberg table** — append only, to an already-existing
table (no Iceberg equivalent of `TableCreator`/`TableMerger`/`Compactor`/
`Vacuum` yet):

```java
import com.labuk.labukdb.iceberg.IcebergTableWriter;

new IcebergTableWriter(location).appendBatch(batch, Map.of("level", "ERROR"));
```

Same `appendBatch(VectorSchemaRoot, Map<String,String>)` shape as
`CommitWriter`, deliberately, but no OCC retry loop of its own —
`iceberg-core`'s own `Table.newAppend().commit()` already retries
internally under a concurrent-writer conflict (confirmed against its
real bytecode), so there's nothing to hand-roll the way Delta's
`CommitWriter` has to. `IcebergTableWriter` also has the same
catalog-based constructor as `IcebergTableFormat` —
`new IcebergTableWriter(catalog, identifier)` — for appending to a
catalog-registered table instead of a path-based one; see
[Iceberg via a REST catalog](#iceberg-via-a-rest-catalog) above.

### Configuring a remote backend

No env-var auto-detection at the library layer (that convenience is
CLI/Flight-only) — build the `Configuration` yourself:

```java
import com.labuk.labukdb.core.storage.MinioConfiguration;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.hadoop.conf.Configuration;

Configuration conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
// or AdlsConfiguration.forSharedKey(account, key) / .forServicePrincipal(...)
TableLocation location = TableLocation.forRemote("s3a://bucket/path", conf);
```

`MinioConfiguration.fromEnvironment()` / `AdlsConfiguration.fromEnvironment()`
are available too, if you want the same env-var convenience the CLI/Flight
server use internally (`MINIO_ENDPOINT`/`MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY`,
`ADLS_STORAGE_ACCOUNT`/`ADLS_AUTH_TYPE`/...) — see
[`ARCHITECTURE.md`'s Storage backends](ARCHITECTURE.md#storage-backends)
section for the full environment-variable table.

---

## Choosing a mode

- **Quick terminal lookup, scripting** → CLI, flags (section 1).
- **Real SQL from a terminal, no server to run** → CLI, SQL (section 2).
- **Multiple remote clients, concurrent readers/writers, any language** →
  Flight server (section 3).
- **Embedding directly inside a JVM app, no IPC hop** → embedded library
  (section 4).

All four are query-equivalent for `SELECT`/filter/`GROUP BY`/`ORDER BY`/
`LIMIT` — none trades away correctness for convenience. Writes are
reachable from the embedded library and Flight's `DoPut`: Delta has the
full set (`CommitWriter`/`TableCreator`/`TableMerger`/`Compactor`/
`Vacuum`), Iceberg has append-only (`IcebergTableWriter`, no create/
merge/compact/vacuum equivalent yet). The CLI is query-only for both
formats — no `ingest`/`compact`/`vacuum`/`create-table` subcommands.
