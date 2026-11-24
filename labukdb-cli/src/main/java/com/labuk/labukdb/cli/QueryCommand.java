package com.labuk.labukdb.cli;

import com.labuk.labukdb.core.catalog.TableCatalog;
import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import com.labuk.labukdb.core.execution.AggregateOperator;
import com.labuk.labukdb.core.execution.AggregateSpec;
import com.labuk.labukdb.core.execution.CombinedQueryExecutor;
import com.labuk.labukdb.core.execution.FileScan;
import com.labuk.labukdb.core.execution.HashJoinOperator;
import com.labuk.labukdb.core.execution.JoinType;
import com.labuk.labukdb.core.execution.RowDeduplicator;
import com.labuk.labukdb.core.execution.SortKey;
import com.labuk.labukdb.core.execution.SortLimitOperator;
import com.labuk.labukdb.core.format.DataFileRef;
import com.labuk.labukdb.core.format.DeltaTableFormat;
import com.labuk.labukdb.core.format.DetectedFormat;
import com.labuk.labukdb.core.format.NormalizedSnapshot;
import com.labuk.labukdb.core.format.TableFormat;
import com.labuk.labukdb.core.log.Snapshot;
import com.labuk.labukdb.core.log.SnapshotManager;
import com.labuk.labukdb.core.log.action.AddFile;
import com.labuk.labukdb.core.logical.LogicalPlan;
import com.labuk.labukdb.core.logical.LogicalPlanBuilder;
import com.labuk.labukdb.core.logical.LogicalPlanExecutor;
import com.labuk.labukdb.core.logical.JoinExecution;
import com.labuk.labukdb.core.logical.ResolvedScan;
import com.labuk.labukdb.core.logical.RuleBasedOptimizer;
import com.labuk.labukdb.core.planner.NormalizedScanPlan;
import com.labuk.labukdb.core.planner.PartitionValueSubstituter;
import com.labuk.labukdb.core.planner.Predicate;
import com.labuk.labukdb.core.planner.QueryPlanner;
import com.labuk.labukdb.core.planner.ScanPlan;
import com.labuk.labukdb.core.query.SqlQuery;
import com.labuk.labukdb.core.query.SqlQueryTranslator;
import com.labuk.labukdb.core.query.SqlStatement;
import com.labuk.labukdb.core.storage.TableLocation;
import com.labuk.labukdb.iceberg.IcebergRestConfiguration;
import com.labuk.labukdb.iceberg.IcebergRestLocation;
import com.labuk.labukdb.iceberg.IcebergTableFormat;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@code delta-cli query --table <name-or-path> [options]}
 * {@code delta-cli query --sql "<SELECT statement>"}
 *
 * Runs a query directly through log/planner/decode, in-process — no Flight
 * server needed. Exercises exactly the same engine {@code LabukDbFlightProducer}
 * calls, just without gRPC in between. The {@code --sql} path reuses the same
 * {@link CombinedQueryExecutor} Flight's combine-mode {@code DoGet} uses, so
 * {@code GROUP BY}/{@code ORDER BY}/{@code LIMIT} behave identically here.
 */
final class QueryCommand {

    private QueryCommand() {
    }

    static void run(String[] args) throws Exception {
        CliArgs parsed = CliArgs.parse(args);
        if (parsed == null) {
            CliArgs.printUsage();
            System.exit(1);
            return;
        }

        if (parsed.sql != null) {
            runSql(parsed);
            return;
        }

        TableCatalog catalog = TableCatalog.atDefaultLocation();
        String raw = catalog.resolve(parsed.table).orElse(parsed.table);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            if (IcebergRestLocation.isRestUri(raw)) {
                runIceberg(openIcebergRest(raw), raw, parsed.predicate, parsed.columns, List.of(), List.of(),
                        List.of(), -1, parsed.limit > 0 ? parsed.limit : Integer.MAX_VALUE, parsed.limit,
                        parsed.version, parsed.asOf, allocator);
                return;
            }

            TableLocation tableRoot = TableResolver.resolve(parsed.table, catalog);
            if (DetectedFormat.detect(tableRoot) == DetectedFormat.ICEBERG) {
                runIceberg(new IcebergTableFormat(tableRoot), tableRoot.toString(), parsed.predicate, parsed.columns,
                        List.of(), List.of(), List.of(), -1,
                        parsed.limit > 0 ? parsed.limit : Integer.MAX_VALUE, parsed.limit,
                        parsed.version, parsed.asOf, allocator);
                return;
            }

            SnapshotManager snapshotManager = SnapshotManager.forLocation(tableRoot);
            Snapshot snapshot = resolveSnapshot(snapshotManager, parsed.version, parsed.asOf);

            ScanPlan plan = new QueryPlanner().plan(snapshot, parsed.predicate, parsed.columns);
            printTableAndFileSummary(tableRoot, snapshot, plan);
            scanAndPrintPerFile(tableRoot, plan, allocator, parsed.limit > 0 ? parsed.limit : Integer.MAX_VALUE,
                    parsed.limit);
        }
    }

    /**
     * {@code --sql}: {@link SqlQueryTranslator} gives the exact {@link Predicate}/
     * {@link AggregateSpec}/{@link SortKey} structures the engine already
     * executes correctly. {@link LogicalPlanBuilder}/{@link RuleBasedOptimizer}/
     * {@link LogicalPlanExecutor} (new — see {@code core.logical}'s own
     * javadoc for the full SQL-engine-architecture Phase 1 writeup) resolve
     * the query against whichever table format is actually at the table
     * name — one path for Delta, Iceberg, and REST-catalog Iceberg alike,
     * replacing what used to be three separately-duplicated branches here.
     * A plain {@code SELECT}+{@code WHERE} (no {@code GROUP BY}/aggregates/
     * {@code ORDER BY}/{@code LIMIT}) reuses the existing per-file streaming
     * scan, for *either* format now (previously only Delta did this —
     * Iceberg always fully materialized via {@link CombinedQueryExecutor}
     * regardless of need); anything needing cross-file combination goes
     * through {@link CombinedQueryExecutor} — same {@code combineMode}
     * trigger {@code LabukDbFlightProducer} uses, for the same reason
     * (those can't be answered correctly file-by-file). Unlike
     * {@code --limit} (which defaults to capping output at 50 rows), a bare
     * SQL {@code SELECT} with no {@code LIMIT} clause prints every matching
     * row — that is what "no LIMIT" means in SQL, and silently capping it
     * would misrepresent the result.
     */
    private static void runSql(CliArgs parsed) throws Exception {
        SqlStatement statement = SqlQueryTranslator.translateAny(parsed.sql);
        TableCatalog catalog = TableCatalog.atDefaultLocation();

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            if (statement instanceof SqlStatement.Union union) {
                runSqlUnion(union, catalog, parsed, allocator);
                return;
            }
            SqlQuery rawQuery = ((SqlStatement.Single) statement).query();
            Optional<SqlQuery> resolvedQuery = resolveScalarSubqueries(rawQuery, catalog, parsed, allocator);
            if (resolvedQuery.isEmpty()) {
                // SQL engine Phase 4 increment 2: a scalar subquery resolved
                // to zero rows (or a NULL value) -- "x OP NULL" is never
                // true for any comparison operator, so the whole outer
                // query is unconditionally zero rows, same observable
                // result standard SQL's three-valued logic produces.
                System.out.println("0 row(s)");
                return;
            }
            SqlQuery query = resolvedQuery.get();
            LogicalPlan plan = RuleBasedOptimizer.DEFAULT.optimize(LogicalPlanBuilder.build(query));

            if (!query.joins().isEmpty()) {
                runSqlJoin(query, plan, catalog, parsed, allocator);
                return;
            }

            ResolvedTable resolvedTable = resolveTableFormat(query.table(), catalog);
            ResolvedScan resolved = new LogicalPlanExecutor().resolve(plan, resolvedTable.format(),
                    resolvedTable.dataFileRoot(), parsed.version, parsed.asOf);
            printResolvedSummary(resolvedTable.displayName(), resolvedTable.versionLabel(), resolved);

            if (resolved.files().isEmpty()) {
                System.out.println("0 row(s)");
                return;
            }

            if (resolved.needsCombine()) {
                List<String> groupByColumns = resolved.aggregate() != null
                        ? resolved.aggregate().groupByColumns() : List.of();
                List<AggregateSpec> aggregates = resolved.aggregate() != null
                        ? resolved.aggregate().aggregates() : List.of();
                List<SortKey> sortKeys = resolved.sort() != null ? resolved.sort().sortKeys() : List.of();
                long limitValue = resolved.limit() != null ? resolved.limit().limit() : -1;

                try (VectorSchemaRoot result = new CombinedQueryExecutor().execute(resolved.files(),
                        resolved.columns(), groupByColumns, aggregates, sortKeys, limitValue, allocator)) {
                    printHeader(result);
                    printRows(result, result.getRowCount());
                    System.out.println();
                    System.out.println(result.getRowCount() + " row(s)");
                }
            } else if (resolved.hasLimitOnly()) {
                // SQL engine Phase 2: no ORDER BY/GROUP BY, so "any N matching
                // rows" can stop as soon as N are found -- never opens a later
                // file once satisfied, unlike CombinedQueryExecutor.
                scanAndPrintUpToLimit(resolved.files(), resolved.columns(), resolved.limit().limit(), allocator);
            } else {
                scanAndPrintAllFiles(resolved.files(), resolved.columns(), allocator);
            }
        }
    }

    /** One table name resolved into an already-open format, the location its data files resolve against, and display metadata. */
    private record ResolvedTable(TableFormat format, TableLocation dataFileRoot, String displayName, String versionLabel) {
    }

    /**
     * The same Delta-vs-Iceberg-vs-REST-Iceberg detection {@code runSql}'s
     * single-table path always used — extracted here specifically so SQL
     * engine Phase 3's {@link #runSqlJoin} can run it twice, once per
     * side, with zero duplicated detection logic.
     */
    private static ResolvedTable resolveTableFormat(String tableName, TableCatalog catalog) throws Exception {
        String raw = catalog.resolve(tableName).orElse(tableName);

        if (IcebergRestLocation.isRestUri(raw)) {
            IcebergTableFormat icebergFormat = openIcebergRest(raw);
            return new ResolvedTable(icebergFormat,
                    TableLocation.fromStringWithEnvironmentConfig(icebergFormat.location()), raw, "Iceberg snapshot");
        }

        TableLocation tableRoot = TableResolver.resolve(tableName, catalog);
        if (DetectedFormat.detect(tableRoot) == DetectedFormat.ICEBERG) {
            IcebergTableFormat icebergFormat = new IcebergTableFormat(tableRoot);
            return new ResolvedTable(icebergFormat,
                    TableLocation.fromStringWithEnvironmentConfig(icebergFormat.location()),
                    tableRoot.toString(), "Iceberg snapshot");
        }
        return new ResolvedTable(new DeltaTableFormat(tableRoot), tableRoot, tableRoot.toString(), "version");
    }

    /**
     * SQL engine Phase 4 increment 2: resolves every {@code
     * query.scalarSubqueries()} once, merging each one's single resulting
     * value into an ordinary {@code Predicate.Comparison} on the right
     * table's own predicate bucket, before the outer {@code LogicalPlan}
     * is ever built -- {@code SqlQueryTranslator} parsed these but
     * couldn't run them (no {@code TableFormat} access there). Returns
     * {@link Optional#empty()} when any subquery resolves to zero rows or
     * a {@code NULL} value -- {@code x OP NULL} is never {@code true} for
     * any comparison operator, so the whole outer query is unconditionally
     * zero rows regardless of which subquery produced it.
     *
     * Also resolves each JoinStep's own scalarSubqueries() -- a scalar
     * subquery nested inside an IN/EXISTS subquery's own filter.
     */
    private static Optional<SqlQuery> resolveScalarSubqueries(SqlQuery query, TableCatalog catalog, CliArgs parsed,
                                                                 BufferAllocator allocator) throws Exception {
        if (query.scalarSubqueries().isEmpty() && query.joins().stream().allMatch(s -> s.scalarSubqueries().isEmpty())) {
            return Optional.of(query);
        }

        Map<String, Predicate> extraPerAlias = new LinkedHashMap<>();
        for (SqlQuery.ScalarSubquery sub : query.scalarSubqueries()) {
            Comparable<?> value = resolveScalarSubqueryValue(sub.innerQuery(), catalog, parsed, allocator);
            if (value == null) {
                return Optional.empty();
            }
            Predicate cmp = Predicate.compare(sub.outerColumn(), sub.op(), value);
            extraPerAlias.merge(sub.outerTableAlias(), cmp, Predicate::and);
        }
        // A scalar subquery nested inside an IN/EXISTS subquery's own filter
        // (e.g. EXISTS (SELECT 1 FROM b WHERE b.id = a.id AND b.x = (SELECT
        // MAX(c.y) FROM c))) -- its outerTableAlias() is always that step's
        // own rightAlias(), so it merges into this same map and the per-step
        // distribution below (keyed by rightAlias()) already routes it
        // correctly, with zero extra logic needed.
        for (SqlQuery.JoinStep step : query.joins()) {
            for (SqlQuery.ScalarSubquery sub : step.scalarSubqueries()) {
                Comparable<?> value = resolveScalarSubqueryValue(sub.innerQuery(), catalog, parsed, allocator);
                if (value == null) {
                    return Optional.empty();
                }
                Predicate cmp = Predicate.compare(sub.outerColumn(), sub.op(), value);
                extraPerAlias.merge(sub.outerTableAlias(), cmp, Predicate::and);
            }
        }

        Predicate newBasePredicate = query.predicate();
        Predicate baseExtra = extraPerAlias.remove(query.baseAlias());
        if (baseExtra != null) {
            newBasePredicate = newBasePredicate == null ? baseExtra : Predicate.and(newBasePredicate, baseExtra);
        }

        List<SqlQuery.JoinStep> newSteps = new ArrayList<>();
        for (SqlQuery.JoinStep step : query.joins()) {
            Predicate stepExtra = extraPerAlias.remove(step.rightAlias());
            Predicate newRightPredicate = stepExtra == null ? step.rightPredicate()
                    : step.rightPredicate() == null ? stepExtra : Predicate.and(step.rightPredicate(), stepExtra);
            newSteps.add(new SqlQuery.JoinStep(step.type(), step.rightTable(), step.rightAlias(), step.keys(),
                    newRightPredicate, List.of()));
        }

        if (!extraPerAlias.isEmpty()) {
            // Shouldn't happen -- every ScalarSubquery.outerTableAlias() came
            // from an alias SqlQueryTranslator already validated against this
            // same query's base/joins. Defensive, not a reachable user error.
            throw new IllegalStateException("Scalar subquery referenced an unknown table alias: " + extraPerAlias.keySet());
        }

        List<String> finalColumns = query.columns();
        List<String> finalGroupBy = query.groupByColumns();
        List<AggregateSpec> finalAggregates = query.aggregates();
        List<SortKey> finalOrderBy = query.orderBy();
        String finalBaseAlias = query.baseAlias();
        if (newSteps.isEmpty()) {
            // This query only took the alias-qualified, join-shaped
            // translation path because of a scalar subquery (not a real
            // JOIN/IN/EXISTS) -- with no JoinStep surviving, no
            // HashJoinOperator call will ever run to actually produce
            // those alias-qualified names, so SqlQueryTranslator's
            // "a.col"-style names must be stripped back to bare ones here,
            // matching what the single-table execution path (which this
            // query is about to fall into) has always expected. Predicate
            // itself never needed this -- its own column references were
            // already bare (toPredicate/columnName use Column.getColumnName(),
            // not getFullyQualifiedName()); only the SELECT/GROUP BY/
            // ORDER BY/aggregate column *lists* were ever qualified.
            finalColumns = stripAliases(query.columns());
            finalGroupBy = stripAliases(query.groupByColumns());
            finalAggregates = query.aggregates().stream()
                    .map(a -> new AggregateSpec(stripAlias(a.column()), a.function(), a.outputName()))
                    .toList();
            finalOrderBy = query.orderBy().stream()
                    .map(k -> new SortKey(stripAlias(k.column()), k.ascending()))
                    .toList();
            finalBaseAlias = null;
        }

        return Optional.of(new SqlQuery(query.table(), finalBaseAlias, finalColumns, newBasePredicate,
                finalGroupBy, finalAggregates, finalOrderBy, query.limit(), newSteps, List.of()));
    }

    private static List<String> stripAliases(List<String> qualifiedNames) {
        return qualifiedNames.stream().map(QueryCommand::stripAlias).toList();
    }

    private static String stripAlias(String qualifiedName) {
        int dot = qualifiedName.indexOf('.');
        return dot < 0 ? qualifiedName : qualifiedName.substring(dot + 1);
    }

    /**
     * Resolves one scalar subquery to its single value, reusing the exact
     * same single-table primitives {@link #runSql} itself uses for a
     * top-level query ({@link #resolveTableFormat}, {@link
     * LogicalPlanExecutor#resolve}, {@link CombinedQueryExecutor}) -- no
     * new execution primitive, just without the printing. Recurses via
     * {@link #resolveScalarSubqueries} first so an arbitrarily-nested
     * scalar subquery resolves correctly instead of being silently
     * dropped ({@code LogicalPlanBuilder} never looks at {@code
     * scalarSubqueries()} itself -- skipping this recursive call would be
     * a real, silent correctness bug for a doubly-nested case, not
     * attempted).
     *
     * @return the single resolved value, or {@code null} if the subquery
     *         resolved to zero rows or its one cell is itself {@code NULL}
     *         (both cases mean "the comparison can never be true" --
     *         handled identically by the caller).
     */
    private static Comparable<?> resolveScalarSubqueryValue(SqlQuery innerQueryRaw, TableCatalog catalog, CliArgs parsed,
                                                               BufferAllocator allocator) throws Exception {
        Optional<SqlQuery> resolvedInner = resolveScalarSubqueries(innerQueryRaw, catalog, parsed, allocator);
        if (resolvedInner.isEmpty()) {
            return null;
        }
        SqlQuery innerQuery = resolvedInner.get();

        if (!innerQuery.joins().isEmpty()) {
            throw new IllegalArgumentException("A scalar subquery with its own JOIN is not supported yet: " + innerQuery.table());
        }
        if (!innerQuery.groupByColumns().isEmpty()) {
            throw new IllegalArgumentException("A scalar subquery with GROUP BY is not supported yet "
                    + "(could produce more than one row): " + innerQuery.table());
        }
        boolean singlePlainColumn = innerQuery.aggregates().isEmpty() && innerQuery.columns().size() == 1;
        boolean singleAggregate = innerQuery.aggregates().size() == 1 && innerQuery.columns().isEmpty();
        if (!singlePlainColumn && !singleAggregate) {
            throw new IllegalArgumentException("A scalar subquery must produce exactly one column: " + innerQuery.table());
        }

        LogicalPlan innerPlan = RuleBasedOptimizer.DEFAULT.optimize(LogicalPlanBuilder.build(innerQuery));
        ResolvedTable resolvedTable = resolveTableFormat(innerQuery.table(), catalog);
        ResolvedScan resolved = new LogicalPlanExecutor().resolve(innerPlan, resolvedTable.format(),
                resolvedTable.dataFileRoot(), parsed.version, parsed.asOf);

        if (resolved.files().isEmpty()) {
            if (singleAggregate) {
                return switch (innerQuery.aggregates().get(0).function()) {
                    case SUM -> 0.0;
                    case COUNT -> 0L;
                    case AVG, MIN, MAX -> null;
                };
            }
            return null;
        }

        List<String> groupByColumns = resolved.aggregate() != null ? resolved.aggregate().groupByColumns() : List.of();
        List<AggregateSpec> aggregates = resolved.aggregate() != null ? resolved.aggregate().aggregates() : List.of();
        List<SortKey> sortKeys = resolved.sort() != null ? resolved.sort().sortKeys() : List.of();
        long limitValue = resolved.limit() != null ? resolved.limit().limit() : -1;

        try (VectorSchemaRoot result = new CombinedQueryExecutor().execute(resolved.files(), resolved.columns(),
                groupByColumns, aggregates, sortKeys, limitValue, allocator)) {
            if (result.getRowCount() == 0) {
                return null;
            }
            if (result.getRowCount() > 1) {
                throw new IllegalArgumentException("Scalar subquery returned more than one row ("
                        + result.getRowCount() + ") -- expected exactly one: " + innerQuery.table());
            }
            List<FieldVector> vectors = result.getFieldVectors();
            if (vectors.size() != 1) {
                throw new IllegalArgumentException("Scalar subquery must produce exactly one column, got "
                        + vectors.size() + ": " + innerQuery.table());
            }
            return scalarCellValue(vectors.get(0));
        }
    }

    /** Same boxed-value pattern {@code HashJoinOperator}/{@code AggregateOperator} already use for their own key/value extraction -- a null cell returns null, same "never matches" meaning the caller already handles for zero rows. */
    private static Comparable<?> scalarCellValue(FieldVector vector) {
        if (vector.isNull(0)) {
            return null;
        }
        if (vector instanceof org.apache.arrow.vector.IntVector v) {
            return (long) v.get(0);
        }
        if (vector instanceof org.apache.arrow.vector.BigIntVector v) {
            return v.get(0);
        }
        if (vector instanceof org.apache.arrow.vector.Float4Vector v) {
            return (double) v.get(0);
        }
        if (vector instanceof org.apache.arrow.vector.Float8Vector v) {
            return v.get(0);
        }
        if (vector instanceof org.apache.arrow.vector.VarCharVector v) {
            return new String(v.get(0), java.nio.charset.StandardCharsets.UTF_8);
        }
        throw new IllegalArgumentException("Scalar subquery's result column type isn't supported as a comparison literal yet: "
                + vector.getClass().getSimpleName());
    }

    /**
     * A left-deep chain of one or more equi-joins ({@code INNER}/{@code
     * LEFT}/{@code RIGHT}/{@code FULL}). Every table is resolved and
     * fully materialized completely independently (reusing {@link
     * CombinedQueryExecutor} with empty {@code groupBy}/{@code
     * aggregates}/{@code sortKeys} and {@code limit=-1}, which reduces to
     * exactly "decode every surviving file and concatenate" — a join
     * inherently needs to see every row of at least one side anyway, the
     * same assumption {@code GROUP BY}/{@code ORDER BY} already make),
     * then {@link HashJoinOperator} folds them together one step at a
     * time, left to right, feeding each step's combined output back in
     * as the next step's "accumulated" side.
     *
     * <b>Cost-based build/probe side, {@code INNER} steps only:</b> compares
     * the running accumulated side's total file bytes (summed across every
     * table joined so far) against the new table's own total bytes (both
     * from {@link ResolvedScan#totalBytes()}) and builds {@code
     * HashJoinOperator}'s hash table from whichever is smaller — a byte-size
     * heuristic, not real cardinality-based costing (no uniform row-count
     * signal exists across both formats yet), and applied greedily per step,
     * not as join-order reordering. {@code LEFT}/{@code RIGHT}/{@code FULL}
     * steps keep the fixed non-preserved-side-builds convention — see
     * {@code HashJoinOperator}'s own javadoc for why that stays decoupled
     * from this heuristic.
     *
     * <b>Empty-table short-circuit, and its one known narrow gap:</b> any
     * table with zero surviving files prints "0 row(s)" unconditionally,
     * matching {@code INNER}'s always-correct behavior. For {@code LEFT}/
     * {@code FULL}, this is technically incomplete — a *wholly empty*
     * non-preserved table should still null-pad every preserved row rather
     * than produce zero rows. Not fixed here: {@code CombinedQueryExecutor}
     * returns {@code null} for zero files (confirmed by reading it), since
     * it has no file to read a schema from and nothing else here carries
     * one — synthesizing a correctly-typed empty root without that would
     * need new schema-sourcing plumbing, out of scope for this phase. This
     * is narrow and distinct from the *common* unmatched-row case (a table
     * with >=1 file whose rows simply don't match the join key), which
     * {@code HashJoinOperator} already handles correctly as a natural
     * consequence of its own zero-match loop bounds — that's the normal
     * path this short-circuit does NOT affect.
     *
     * Optional {@code GROUP BY}/aggregates/{@code ORDER BY}/{@code LIMIT}
     * on top of the joined result reuse the exact same {@code
     * AggregateOperator}/{@code SortLimitOperator} calls {@link
     * CombinedQueryExecutor#execute} already makes for the single-table
     * path. When nothing aggregated, the final {@code SELECT} list
     * (already the alias-qualified names {@code HashJoinOperator}'s own
     * output uses, e.g. {@code "a.service"}) selects the printed columns
     * via a zero-copy {@link VectorSchemaRoot} view over the last root's
     * own vectors — never closed itself, since it shares (not copies)
     * that root's buffers, which the surrounding {@code finally} closes.
     */
    private static void runSqlJoin(SqlQuery query, LogicalPlan plan, TableCatalog catalog, CliArgs parsed,
                                     BufferAllocator allocator) throws Exception {
        List<ResolvedTable> tables = new ArrayList<>();
        tables.add(resolveTableFormat(query.table(), catalog));
        for (SqlQuery.JoinStep step : query.joins()) {
            tables.add(resolveTableFormat(step.rightTable(), catalog));
        }

        JoinExecution execution = new LogicalPlanExecutor().resolveJoin(plan,
                tables.stream().map(ResolvedTable::format).toList(),
                tables.stream().map(ResolvedTable::dataFileRoot).toList(),
                parsed.version, parsed.asOf);

        System.out.println(tables.get(0).displayName() + " (" + tables.get(0).versionLabel() + " "
                + execution.base().snapshotVersion() + ") -- " + execution.base().files().size() + " / "
                + execution.base().totalFilesInSnapshot() + " files");
        for (int i = 0; i < execution.steps().size(); i++) {
            ResolvedTable table = tables.get(i + 1);
            ResolvedScan resolved = execution.steps().get(i).table();
            System.out.println("+ " + table.displayName() + " (" + table.versionLabel() + " "
                    + resolved.snapshotVersion() + ") -- " + resolved.files().size() + " / "
                    + resolved.totalFilesInSnapshot() + " files");
        }
        System.out.println();

        boolean anyEmpty = execution.base().files().isEmpty()
                || execution.steps().stream().anyMatch(s -> s.table().files().isEmpty());
        if (anyEmpty) {
            System.out.println("0 row(s)");
            return;
        }

        VectorSchemaRoot current = new CombinedQueryExecutor().execute(execution.base().files(),
                execution.base().columns(), List.of(), List.of(), List.of(), -1, allocator);
        try {
            String currentAlias = query.baseAlias();
            long accumulatedBytes = execution.base().totalBytes();

            for (int i = 0; i < execution.steps().size(); i++) {
                JoinExecution.Step step = execution.steps().get(i);
                SqlQuery.JoinStep sqlStep = query.joins().get(i);

                try (VectorSchemaRoot newTableRoot = new CombinedQueryExecutor().execute(step.table().files(),
                        step.table().columns(), List.of(), List.of(), List.of(), -1, allocator)) {

                    boolean buildFromLeft = step.type() == JoinType.INNER && accumulatedBytes <= step.table().totalBytes();

                    VectorSchemaRoot joined = new HashJoinOperator().join(step.type(),
                            current, step.leftKeys(), currentAlias,
                            newTableRoot, step.rightKeys(), sqlStep.rightAlias(),
                            buildFromLeft, allocator);
                    current.close();
                    current = joined;
                    currentAlias = null; // every later step's accumulated side is already alias-qualified
                    accumulatedBytes += step.table().totalBytes();
                }
            }

            if (execution.aggregate() != null) {
                VectorSchemaRoot aggregated = new AggregateOperator().aggregate(
                        List.of(current), execution.aggregate().groupByColumns(),
                        execution.aggregate().aggregates(), allocator);
                current.close();
                current = aggregated;
            }
            if (execution.sort() != null || execution.limit() != null) {
                List<SortKey> sortKeys = execution.sort() != null ? execution.sort().sortKeys() : List.of();
                long limitValue = execution.limit() != null ? execution.limit().limit() : -1;
                VectorSchemaRoot sorted = new SortLimitOperator().sortAndLimit(current, sortKeys, limitValue, allocator);
                current.close();
                current = sorted;
            }

            // An aggregate's own output (groupByColumns ++ aggregate
            // outputs) is already the final shape -- same reasoning as
            // the single-table path, so this zero-copy trim only runs
            // when nothing aggregated the join chain's wider combined columns.
            VectorSchemaRoot toPrint = execution.aggregate() == null
                    ? new VectorSchemaRoot(query.columns().stream().map(current::getVector).toList())
                    : current;
            printHeader(toPrint);
            printRows(toPrint, toPrint.getRowCount());
            System.out.println();
            System.out.println(toPrint.getRowCount() + " row(s)");
        } finally {
            current.close();
        }
    }

    /**
     * {@code UNION}/{@code UNION ALL}: materializes each branch
     * independently via {@link #materializeForUnion} -- reusing exactly
     * the same single-table/join resolution {@link #runSql} itself uses,
     * just without its streaming fast paths or inline printing, since a
     * UNION branch's result has to be held in memory to concatenate with
     * its siblings anyway -- concatenates them with {@link
     * VectorSchemaRootAppender} (the same concatenation primitive {@link
     * CombinedQueryExecutor}/{@code core.log.Compactor} already use), and
     * for plain {@code UNION} (not {@code ALL}) removes duplicate rows via
     * {@link RowDeduplicator}. Prints once, over the combined (deduplicated,
     * if applicable) result -- or "0 row(s)" with no header at all if every
     * branch came back empty, same convention {@link #runSql}'s own
     * zero-files early-out already uses.
     */
    private static void runSqlUnion(SqlStatement.Union union, TableCatalog catalog, CliArgs parsed,
                                      BufferAllocator allocator) throws Exception {
        List<VectorSchemaRoot> branchResults = new ArrayList<>();
        try {
            for (SqlQuery branch : union.branches()) {
                materializeForUnion(branch, catalog, parsed, allocator).ifPresent(branchResults::add);
            }

            if (branchResults.isEmpty()) {
                System.out.println("0 row(s)");
                return;
            }

            VectorSchemaRoot combined = VectorSchemaRoot.create(branchResults.get(0).getSchema(), allocator);
            combined.allocateNew();
            try {
                for (VectorSchemaRoot branchResult : branchResults) {
                    if (branchResult.getSchema().getFields().size() != combined.getSchema().getFields().size()) {
                        throw new IllegalArgumentException("Every UNION branch must select the same number of "
                                + "columns: expected " + combined.getSchema().getFields().size() + ", found "
                                + branchResult.getSchema().getFields().size());
                    }
                    VectorSchemaRootAppender.append(combined, branchResult);
                }

                VectorSchemaRoot result = union.all() ? combined : new RowDeduplicator().deduplicate(combined, allocator);
                try {
                    printHeader(result);
                    printRows(result, result.getRowCount());
                    System.out.println();
                    System.out.println(result.getRowCount() + " row(s)");
                } finally {
                    if (result != combined) {
                        result.close();
                    }
                }
            } finally {
                combined.close();
            }
        } finally {
            for (VectorSchemaRoot branchResult : branchResults) {
                branchResult.close();
            }
        }
    }

    /**
     * Resolves and fully materializes one UNION branch into a single
     * {@link VectorSchemaRoot} -- reusing the exact single-table/join
     * resolution {@link #runSql}/{@link #runSqlJoin} themselves use, just
     * without the printing and without either streaming fast path (a bare
     * scan or a bare-LIMIT scan): a UNION branch's result has to be fully
     * held in memory to concatenate with its siblings regardless, so there
     * is no streaming benefit to preserve here. Returns {@link
     * Optional#empty()} for zero matching rows (a scalar subquery
     * resolving to zero rows/NULL, or a resolved scan/join with no files
     * at all) -- same "zero rows" convention {@link #resolveScalarSubqueries}
     * and {@link #runSql} itself already use.
     */
    private static Optional<VectorSchemaRoot> materializeForUnion(SqlQuery rawQuery, TableCatalog catalog,
                                                                     CliArgs parsed, BufferAllocator allocator) throws Exception {
        Optional<SqlQuery> resolvedQuery = resolveScalarSubqueries(rawQuery, catalog, parsed, allocator);
        if (resolvedQuery.isEmpty()) {
            return Optional.empty();
        }
        SqlQuery query = resolvedQuery.get();
        LogicalPlan plan = RuleBasedOptimizer.DEFAULT.optimize(LogicalPlanBuilder.build(query));

        if (!query.joins().isEmpty()) {
            return materializeJoinForUnion(query, plan, catalog, parsed, allocator);
        }

        ResolvedTable resolvedTable = resolveTableFormat(query.table(), catalog);
        ResolvedScan resolved = new LogicalPlanExecutor().resolve(plan, resolvedTable.format(),
                resolvedTable.dataFileRoot(), parsed.version, parsed.asOf);

        if (resolved.files().isEmpty()) {
            return Optional.empty();
        }

        List<String> groupByColumns = resolved.aggregate() != null ? resolved.aggregate().groupByColumns() : List.of();
        List<AggregateSpec> aggregates = resolved.aggregate() != null ? resolved.aggregate().aggregates() : List.of();
        List<SortKey> sortKeys = resolved.sort() != null ? resolved.sort().sortKeys() : List.of();
        long limitValue = resolved.limit() != null ? resolved.limit().limit() : -1;

        VectorSchemaRoot result = new CombinedQueryExecutor().execute(resolved.files(), resolved.columns(),
                groupByColumns, aggregates, sortKeys, limitValue, allocator);
        return result == null ? Optional.empty() : Optional.of(result);
    }

    /** {@link #materializeForUnion}'s join-chain counterpart -- same resolution {@link #runSqlJoin} uses, minus printing; see that method's own javadoc for the join-folding shape. */
    private static Optional<VectorSchemaRoot> materializeJoinForUnion(SqlQuery query, LogicalPlan plan, TableCatalog catalog,
                                                                         CliArgs parsed, BufferAllocator allocator) throws Exception {
        List<ResolvedTable> tables = new ArrayList<>();
        tables.add(resolveTableFormat(query.table(), catalog));
        for (SqlQuery.JoinStep step : query.joins()) {
            tables.add(resolveTableFormat(step.rightTable(), catalog));
        }

        JoinExecution execution = new LogicalPlanExecutor().resolveJoin(plan,
                tables.stream().map(ResolvedTable::format).toList(),
                tables.stream().map(ResolvedTable::dataFileRoot).toList(),
                parsed.version, parsed.asOf);

        boolean anyEmpty = execution.base().files().isEmpty()
                || execution.steps().stream().anyMatch(s -> s.table().files().isEmpty());
        if (anyEmpty) {
            return Optional.empty();
        }

        VectorSchemaRoot current = new CombinedQueryExecutor().execute(execution.base().files(),
                execution.base().columns(), List.of(), List.of(), List.of(), -1, allocator);
        try {
            String currentAlias = query.baseAlias();
            long accumulatedBytes = execution.base().totalBytes();

            for (int i = 0; i < execution.steps().size(); i++) {
                JoinExecution.Step step = execution.steps().get(i);
                SqlQuery.JoinStep sqlStep = query.joins().get(i);

                try (VectorSchemaRoot newTableRoot = new CombinedQueryExecutor().execute(step.table().files(),
                        step.table().columns(), List.of(), List.of(), List.of(), -1, allocator)) {

                    boolean buildFromLeft = step.type() == JoinType.INNER && accumulatedBytes <= step.table().totalBytes();

                    VectorSchemaRoot joined = new HashJoinOperator().join(step.type(),
                            current, step.leftKeys(), currentAlias,
                            newTableRoot, step.rightKeys(), sqlStep.rightAlias(),
                            buildFromLeft, allocator);
                    current.close();
                    current = joined;
                    currentAlias = null;
                    accumulatedBytes += step.table().totalBytes();
                }
            }

            if (execution.aggregate() != null) {
                VectorSchemaRoot aggregated = new AggregateOperator().aggregate(
                        List.of(current), execution.aggregate().groupByColumns(),
                        execution.aggregate().aggregates(), allocator);
                current.close();
                current = aggregated;
            }
            if (execution.sort() != null || execution.limit() != null) {
                List<SortKey> sortKeys = execution.sort() != null ? execution.sort().sortKeys() : List.of();
                long limitValue = execution.limit() != null ? execution.limit().limit() : -1;
                VectorSchemaRoot sorted = new SortLimitOperator().sortAndLimit(current, sortKeys, limitValue, allocator);
                current.close();
                current = sorted;
            }

            if (execution.aggregate() == null) {
                // Unlike runSqlJoin (which prints synchronously, then closes
                // current exactly once, in one place), this method hands its
                // result to a caller that holds it open independently and
                // closes it later -- a zero-copy view sharing current's
                // vectors would leave current's *other*, unselected columns
                // (e.g. join keys not in the SELECT list) with no owner left
                // to close them. A genuine copy keeps the two lifetimes
                // fully separate instead.
                return Optional.of(projectColumns(current, query.columns(), allocator));
            }
            VectorSchemaRoot result = current;
            current = null; // ownership transferred to the caller -- finally below must not close it
            return Optional.of(result);
        } finally {
            if (current != null) {
                current.close();
            }
        }
    }

    /** An independent copy of {@code columnNames} from {@code source} -- see {@link #materializeJoinForUnion}'s own javadoc for why this can't be a zero-copy view there. */
    private static VectorSchemaRoot projectColumns(VectorSchemaRoot source, List<String> columnNames, BufferAllocator allocator) {
        List<Field> fields = columnNames.stream().map(name -> source.getVector(name).getField()).toList();
        VectorSchemaRoot result = VectorSchemaRoot.create(new Schema(fields), allocator);
        result.allocateNew();
        int rowCount = source.getRowCount();
        List<FieldVector> resultVectors = result.getFieldVectors();
        for (int c = 0; c < columnNames.size(); c++) {
            FieldVector sourceVector = source.getVector(columnNames.get(c));
            FieldVector resultVector = resultVectors.get(c);
            for (int r = 0; r < rowCount; r++) {
                resultVector.copyFromSafe(r, r, sourceVector);
            }
        }
        result.setRowCount(rowCount);
        return result;
    }

    private static void printResolvedSummary(String displayName, String versionLabel, ResolvedScan resolved) {
        System.out.println("Table: " + displayName + " (" + versionLabel + " " + resolved.snapshotVersion() + ")");
        System.out.println("Files: " + resolved.files().size() + " / " + resolved.totalFilesInSnapshot()
                + " (skipped " + resolved.filesSkipped() + " via partition/file-stats pruning)");
        System.out.println();
    }

    /**
     * The {@code --sql} path's unified per-file streaming print, for either
     * table format — a bare {@code SELECT} with no {@code LIMIT} always
     * prints every row (see {@link #runSql}'s own javadoc), so unlike
     * {@link #scanAndPrintPerFile} (the flag-based {@code --table} path's
     * own version, untouched, which caps output at {@code --limit}), this
     * has no row cap at all.
     */
    private static void scanAndPrintAllFiles(List<FileScan> files, List<String> columns,
                                               BufferAllocator allocator) throws Exception {
        ParquetToArrowReader reader = new ParquetToArrowReader();
        int totalRows = 0;
        boolean headerPrinted = false;

        for (FileScan file : files) {
            try (VectorSchemaRoot root = reader.read(file.dataFile(), allocator, columns,
                    file.residualPredicate(), file.partitionValues())) {
                if (!headerPrinted) {
                    printHeader(root);
                    headerPrinted = true;
                }
                printRows(root, root.getRowCount());
                totalRows += root.getRowCount();
            }
        }

        System.out.println();
        System.out.println(totalRows + " row(s)");
    }

    /**
     * SQL engine Phase 2: a bare {@code LIMIT} with no {@code ORDER BY}/
     * {@code GROUP BY} means "any N matching rows" — stops as soon as
     * {@code limit} rows are collected, the same {@code remaining <= 0}
     * early-stop idiom {@link #scanAndPrintPerFile} (the flag-based
     * {@code --table}/{@code --limit} path) already uses, ported to this
     * path's {@code FileScan}-list signature. Package-private, not
     * {@code private}, specifically so a test can call it directly with a
     * hand-built {@code FileScan} list to prove a later file is genuinely
     * never opened once satisfied — the same package-private-for-testing
     * convention {@code CliArgs}/{@code TableResolver} already use.
     */
    static void scanAndPrintUpToLimit(List<FileScan> files, List<String> columns, long limit,
                                       BufferAllocator allocator) throws Exception {
        ParquetToArrowReader reader = new ParquetToArrowReader();
        long remaining = limit;
        long totalRows = 0;
        boolean headerPrinted = false;

        for (FileScan file : files) {
            if (remaining <= 0) {
                break; // the real proof point -- a later file's TableLocation is never even opened
            }
            try (VectorSchemaRoot root = reader.read(file.dataFile(), allocator, columns,
                    file.residualPredicate(), file.partitionValues())) {
                if (!headerPrinted) {
                    printHeader(root);
                    headerPrinted = true;
                }
                int toPrint = (int) Math.min(root.getRowCount(), remaining);
                printRows(root, toPrint);
                totalRows += toPrint;
                remaining -= toPrint;
            }
        }

        System.out.println();
        System.out.println(totalRows + " row(s)");
    }

    /**
     * REST-catalog counterpart of a plain {@code TableLocation}-based
     * Iceberg open — connects via {@link IcebergRestConfiguration}
     * (env-var-configured, one catalog connection per process) and
     * parses the {@code icebergrest://} reference via {@link
     * IcebergRestLocation}. {@code raw} must already satisfy {@link
     * IcebergRestLocation#isRestUri} — checked by the caller first,
     * before this is reached.
     */
    private static IcebergTableFormat openIcebergRest(String raw) {
        Catalog restCatalog = IcebergRestConfiguration.connect();
        TableIdentifier identifier = IcebergRestLocation.parseTableIdentifier(raw);
        return new IcebergTableFormat(restCatalog, identifier);
    }

    /**
     * The Iceberg counterpart of both {@link #run}'s flag-based path and
     * {@link #runSql}'s combine-mode path, unified into one helper since
     * Iceberg always routes through {@link CombinedQueryExecutor} —
     * deliberately, so it doesn't need its own parallel per-file-streaming
     * implementation the way Delta's plain-scan path has. Delta's own
     * plain-scan-vs-combine-mode split in {@link #run}/{@link #runSql} is
     * completely untouched; this is reached only once {@link
     * DetectedFormat#detect} says the table is Iceberg, or once {@link
     * IcebergRestLocation#isRestUri} says it's a REST-catalog reference.
     * Works identically either way — {@code tableFormat} may have been
     * constructed path-based or catalog-based; this method only ever
     * calls its format-agnostic {@code TableFormat} surface plus the new
     * {@link IcebergTableFormat#location} accessor, which both
     * constructors populate correctly.
     *
     * @param displayName  what to print as "Table: ..." — the original
     *                     {@code TableLocation} for a path-based table,
     *                     or the raw {@code icebergrest://} reference for
     *                     a catalog-based one.
     * @param queryLimit   the real SQL-style {@code LIMIT} passed to {@link
     *                     CombinedQueryExecutor} ({@code -1} = none) — for
     *                     the flag-based path this is always {@code -1};
     *                     row capping there is {@code printCap} instead,
     *                     mirroring how {@code --limit} already behaves for
     *                     Delta (a print cap, not a query {@code LIMIT}).
     * @param printCap     how many of the resulting rows to actually print.
     * @param limitForFooter 0 when {@code printCap} isn't a CLI {@code --limit}
     *                     in play (the SQL path), otherwise the original
     *                     {@code --limit} value, purely for the footer line.
     */
    private static void runIceberg(IcebergTableFormat tableFormat, String displayName, Predicate predicate,
                                    List<String> columns, List<String> groupByColumns,
                                    List<AggregateSpec> aggregates, List<SortKey> orderBy, long queryLimit,
                                    int printCap, int limitForFooter, Long version, Instant asOf,
                                    BufferAllocator allocator) throws Exception {
        NormalizedSnapshot snapshot = version != null ? tableFormat.atVersion(version)
                : asOf != null ? tableFormat.atTimestamp(asOf)
                : tableFormat.latest();
        // Every DataFileRef path Iceberg reports is already absolute (confirmed
        // this session, both path-based and catalog-based), so this is only
        // ever used for its local-vs-remote dispatch, never its string value --
        // deriving it from the table's own reported location works uniformly
        // for both construction paths, not just a TableLocation-supplied one.
        TableLocation dataFileRoot = TableLocation.fromStringWithEnvironmentConfig(tableFormat.location());

        boolean aggregating = !groupByColumns.isEmpty() || !aggregates.isEmpty();
        Set<String> neededColumns = new LinkedHashSet<>();
        if (aggregating) {
            neededColumns.addAll(groupByColumns);
            for (AggregateSpec spec : aggregates) {
                neededColumns.add(spec.column());
            }
        } else {
            if (columns != null) {
                neededColumns.addAll(columns);
            }
            for (SortKey key : orderBy) {
                neededColumns.add(key.column());
            }
        }
        List<String> projectedColumns = List.copyOf(neededColumns);

        NormalizedScanPlan plan = new QueryPlanner().planNormalized(snapshot, predicate, projectedColumns);
        printIcebergSummary(displayName, snapshot, plan);

        if (plan.getFilesToScan().isEmpty()) {
            System.out.println("0 row(s)");
            return;
        }

        List<FileScan> files = new ArrayList<>();
        for (DataFileRef file : plan.getFilesToScan()) {
            TableLocation dataFile = dataFileRoot.resolve(file.getPath());
            Predicate perFileResidual =
                    PartitionValueSubstituter.simplify(plan.getResidualPredicate(), file.getPartitionValues());
            files.add(new FileScan(dataFile, perFileResidual, file.getPartitionValues()));
        }

        try (VectorSchemaRoot result = new CombinedQueryExecutor().execute(files, projectedColumns,
                groupByColumns, aggregates, orderBy, queryLimit, allocator)) {
            printHeader(result);
            int toPrint = Math.min(result.getRowCount(), printCap);
            printRows(result, toPrint);
            System.out.println();
            // The true total is already known here (CombinedQueryExecutor never
            // stops early, unlike Delta's per-file streaming loop), so this can
            // state it exactly rather than Delta's vaguer "more may exist."
            System.out.println(toPrint + " row(s)"
                    + (toPrint < result.getRowCount()
                    ? " shown of " + result.getRowCount() + " total (--limit " + limitForFooter + ")"
                    : ""));
        }
    }

    private static void printIcebergSummary(String displayName, NormalizedSnapshot snapshot, NormalizedScanPlan plan) {
        // Iceberg's "version" is a non-sequential snapshotId, not Delta's small
        // sequential integer -- see IcebergTableFormat's javadoc; stated plainly
        // here rather than printed as if it meant the same thing as Delta's.
        System.out.println("Table: " + displayName + " (Iceberg snapshot " + snapshot.getVersion() + ")");
        System.out.println("Files: " + plan.getFilesToScan().size() + " / " + plan.getTotalFilesInSnapshot()
                + " (skipped " + plan.getFilesSkipped() + " via partition/file-stats pruning)");
        System.out.println();
    }

    private static Snapshot resolveSnapshot(SnapshotManager snapshotManager, Long version, java.time.Instant asOf) {
        return version != null ? snapshotManager.atVersion(version)
                : asOf != null ? snapshotManager.atTimestamp(asOf)
                : snapshotManager.latest();
    }

    private static void printTableAndFileSummary(TableLocation tableRoot, Snapshot snapshot, ScanPlan plan) {
        System.out.println("Table: " + tableRoot + " (version " + snapshot.getVersion() + ")");
        System.out.println("Files: " + plan.getFilesToScan().size() + " / " + plan.getTotalFilesInSnapshot()
                + " (skipped " + plan.getFilesSkipped() + " via partition/file-stats pruning)");
        System.out.println();
    }

    /**
     * @param cliLimitForFooter 0 when no CLI {@code --limit} was in play (the
     *                          SQL path), otherwise the original {@code --limit}
     *                          value, purely to phrase the trailing row-count line.
     */
    private static void scanAndPrintPerFile(TableLocation tableRoot, ScanPlan plan, BufferAllocator allocator,
                                             int remainingLimit, int cliLimitForFooter) throws Exception {
        ParquetToArrowReader reader = new ParquetToArrowReader();
        int totalRows = 0;
        boolean headerPrinted = false;
        int remaining = remainingLimit;

        for (AddFile file : plan.getFilesToScan()) {
            if (remaining <= 0) {
                break;
            }
            TableLocation dataFile = tableRoot.resolve(file.getPath());
            Predicate perFileResidual =
                    PartitionValueSubstituter.simplify(plan.getResidualPredicate(), file.getPartitionValues());

            try (VectorSchemaRoot root = reader.read(dataFile, allocator, plan.getProjectedColumns(),
                    perFileResidual, file.getPartitionValues())) {
                if (!headerPrinted) {
                    printHeader(root);
                    headerPrinted = true;
                }
                int toPrint = Math.min(root.getRowCount(), remaining);
                printRows(root, toPrint);
                totalRows += toPrint;
                remaining -= toPrint;
            }
        }

        System.out.println();
        System.out.println(totalRows + " row(s)"
                + (remaining <= 0 && cliLimitForFooter > 0 ? " (hit --limit " + cliLimitForFooter + ", more may exist)" : ""));
    }

    private static void printHeader(VectorSchemaRoot root) {
        List<String> names = root.getSchema().getFields().stream().map(Field::getName).collect(Collectors.toList());
        System.out.println(String.join(" | ", names));
        System.out.println("-".repeat(names.stream().mapToInt(String::length).sum() + 3 * names.size()));
    }

    private static void printRows(VectorSchemaRoot root, int count) {
        List<FieldVector> vectors = root.getFieldVectors();
        for (int r = 0; r < count; r++) {
            List<String> cells = new ArrayList<>();
            for (FieldVector v : vectors) {
                Object value = v.getObject(r);
                cells.add(value == null ? "NULL" : value.toString());
            }
            System.out.println(String.join(" | ", cells));
        }
    }
}
