package com.labuk.labukdb.cli;

import com.labuk.labukdb.core.catalog.TableCatalog;
import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import com.labuk.labukdb.core.execution.AggregateOperator;
import com.labuk.labukdb.core.execution.AggregateSpec;
import com.labuk.labukdb.core.execution.CombinedQueryExecutor;
import com.labuk.labukdb.core.execution.FileScan;
import com.labuk.labukdb.core.execution.HashJoinOperator;
import com.labuk.labukdb.core.execution.JoinType;
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
import com.labuk.labukdb.core.storage.TableLocation;
import com.labuk.labukdb.iceberg.IcebergRestConfiguration;
import com.labuk.labukdb.iceberg.IcebergRestLocation;
import com.labuk.labukdb.iceberg.IcebergTableFormat;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
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
        SqlQuery query = SqlQueryTranslator.translate(parsed.sql);
        LogicalPlan plan = RuleBasedOptimizer.DEFAULT.optimize(LogicalPlanBuilder.build(query));
        TableCatalog catalog = TableCatalog.atDefaultLocation();

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
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
