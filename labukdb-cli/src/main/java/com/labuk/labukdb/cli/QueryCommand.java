package com.labuk.labukdb.cli;

import com.labuk.labukdb.core.catalog.TableCatalog;
import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import com.labuk.labukdb.core.execution.AggregateSpec;
import com.labuk.labukdb.core.execution.CombinedQueryExecutor;
import com.labuk.labukdb.core.execution.FileScan;
import com.labuk.labukdb.core.execution.SortKey;
import com.labuk.labukdb.core.log.Snapshot;
import com.labuk.labukdb.core.log.SnapshotManager;
import com.labuk.labukdb.core.log.action.AddFile;
import com.labuk.labukdb.core.planner.PartitionValueSubstituter;
import com.labuk.labukdb.core.planner.Predicate;
import com.labuk.labukdb.core.planner.QueryPlanner;
import com.labuk.labukdb.core.planner.ScanPlan;
import com.labuk.labukdb.core.query.SqlQuery;
import com.labuk.labukdb.core.query.SqlQueryTranslator;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

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
 * server needed. Exercises exactly the same engine {@code DeltaFlightProducer}
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
        TableLocation tableRoot = TableResolver.resolve(parsed.table, catalog);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
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
     * executes correctly. A plain {@code SELECT}+{@code WHERE} (no {@code GROUP BY}/
     * aggregates/{@code ORDER BY}/{@code LIMIT}) reuses the existing per-file
     * streaming scan; anything needing cross-file combination goes through
     * {@link CombinedQueryExecutor} — same {@code combineMode} trigger
     * {@code DeltaFlightProducer} uses, for the same reason (those can't be
     * answered correctly file-by-file). Unlike {@code --limit} (which defaults
     * to capping output at 50 rows), a bare SQL {@code SELECT} with no
     * {@code LIMIT} clause prints every matching row — that is what "no LIMIT"
     * means in SQL, and silently capping it would misrepresent the result.
     */
    private static void runSql(CliArgs parsed) throws Exception {
        SqlQuery query = SqlQueryTranslator.translate(parsed.sql);
        boolean combineMode = !query.groupByColumns().isEmpty() || !query.aggregates().isEmpty()
                || !query.orderBy().isEmpty() || query.limit() >= 0;

        TableCatalog catalog = TableCatalog.atDefaultLocation();
        TableLocation tableRoot = TableResolver.resolve(query.table(), catalog);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            SnapshotManager snapshotManager = SnapshotManager.forLocation(tableRoot);
            Snapshot snapshot = resolveSnapshot(snapshotManager, parsed.version, parsed.asOf);

            if (!combineMode) {
                ScanPlan plan = new QueryPlanner().plan(snapshot, query.predicate(), query.columns());
                printTableAndFileSummary(tableRoot, snapshot, plan);
                scanAndPrintPerFile(tableRoot, plan, allocator, Integer.MAX_VALUE, 0);
                return;
            }

            boolean aggregating = !query.groupByColumns().isEmpty() || !query.aggregates().isEmpty();
            Set<String> neededColumns = new LinkedHashSet<>();
            if (aggregating) {
                neededColumns.addAll(query.groupByColumns());
                for (AggregateSpec spec : query.aggregates()) {
                    neededColumns.add(spec.column());
                }
            } else {
                neededColumns.addAll(query.columns());
                for (SortKey key : query.orderBy()) {
                    neededColumns.add(key.column());
                }
            }
            List<String> projectedColumns = List.copyOf(neededColumns);

            ScanPlan plan = new QueryPlanner().plan(snapshot, query.predicate(), projectedColumns);
            printTableAndFileSummary(tableRoot, snapshot, plan);

            if (plan.getFilesToScan().isEmpty()) {
                System.out.println("0 row(s)");
                return;
            }

            List<FileScan> files = new ArrayList<>();
            for (AddFile file : plan.getFilesToScan()) {
                TableLocation dataFile = tableRoot.resolve(file.getPath());
                Predicate perFileResidual =
                        PartitionValueSubstituter.simplify(plan.getResidualPredicate(), file.getPartitionValues());
                files.add(new FileScan(dataFile, perFileResidual, file.getPartitionValues()));
            }

            try (VectorSchemaRoot result = new CombinedQueryExecutor().execute(files, projectedColumns,
                    query.groupByColumns(), query.aggregates(), query.orderBy(), query.limit(), allocator)) {
                printHeader(result);
                printRows(result, result.getRowCount());
                System.out.println();
                System.out.println(result.getRowCount() + " row(s)");
            }
        }
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
