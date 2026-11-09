package com.labuk.labukdb.core.logical;

import com.labuk.labukdb.core.execution.FileScan;
import com.labuk.labukdb.core.format.DataFileRef;
import com.labuk.labukdb.core.format.NormalizedSnapshot;
import com.labuk.labukdb.core.format.TableFormat;
import com.labuk.labukdb.core.planner.NormalizedScanPlan;
import com.labuk.labukdb.core.planner.PartitionValueSubstituter;
import com.labuk.labukdb.core.planner.Predicate;
import com.labuk.labukdb.core.planner.QueryPlanner;
import com.labuk.labukdb.core.storage.TableLocation;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Resolves an optimized {@link LogicalPlan} into the files/metadata needed
 * to actually execute it — one path for any {@link TableFormat}
 * implementation, replacing the 3-way duplicated branching {@code
 * QueryCommand} used to have (Delta streaming, Delta combined, Iceberg —
 * which always combined regardless of need).
 *
 * <b>Deliberately does not open the table itself</b> — format detection
 * (Delta vs. Iceberg vs. REST-catalog Iceberg) needs cross-module
 * visibility into {@code labukdb-format-iceberg} that {@code labukdb-core}
 * doesn't have (and shouldn't: the dependency runs the other way). The
 * caller (today, {@code QueryCommand}, which already depends on both
 * modules) resolves the table name into an already-open {@link
 * TableFormat} plus the {@link TableLocation} its data files resolve
 * against, exactly as it already does today, and hands both in here —
 * this class's job starts at "snapshot resolution" and ends at "files to
 * scan," the part that's genuinely format-agnostic.
 *
 * Returns metadata only — does not execute aggregation, the join itself,
 * or printing; the terminal execution (stream-and-print vs.
 * combine-and-print vs. hash-join-and-print) stays the caller's job.
 */
public final class LogicalPlanExecutor {

    public ResolvedScan resolve(LogicalPlan optimizedPlan, TableFormat format, TableLocation dataFileRoot,
                                 Long version, Instant asOf) throws Exception {
        TableScan scan = findScan(optimizedPlan);
        Aggregate aggregate = findAggregate(optimizedPlan);
        Sort sort = findSort(optimizedPlan);
        Limit limit = findLimit(optimizedPlan);

        ResolvedScan side = resolveSide(scan, format, dataFileRoot, version, asOf);
        return new ResolvedScan(side.files(), side.columns(), aggregate, sort, limit,
                side.snapshotVersion(), side.totalFilesInSnapshot(), side.filesSkipped(), side.totalBytes());
    }

    /**
     * Finds the (possibly multi-step, left-deep) {@link Join} chain in an
     * optimized plan and resolves every table independently via {@link
     * #resolveSide} — the exact same per-side logic the single-table
     * {@link #resolve} uses, run once per table. After optimization every
     * table is guaranteed to be a bare {@link TableScan} ({@link
     * PushFilterIntoScan} has already absorbed any {@link Filter} directly
     * onto it), so each can be resolved completely independently —
     * including, as a free consequence rather than a specifically-built
     * feature, against a different table format per table (Delta mixed
     * with Iceberg, in any position).
     *
     * @param formats, dataRoots one entry per joined table, in the same
     *                 order the SQL introduced them (base table first,
     *                 then each join step's new table) — size must be
     *                 {@code chain length + 1}.
     */
    public JoinExecution resolveJoin(LogicalPlan optimizedPlan, List<TableFormat> formats, List<TableLocation> dataRoots,
                                      Long version, Instant asOf) throws Exception {
        Join outermost = findJoin(optimizedPlan);
        List<Join> chain = flattenJoinChain(outermost);
        TableScan baseScan = baseScanOf(outermost);

        if (formats.size() != chain.size() + 1 || dataRoots.size() != chain.size() + 1) {
            throw new IllegalArgumentException("Expected " + (chain.size() + 1) + " format(s)/location(s) "
                    + "(one per joined table), got " + formats.size() + " format(s) and " + dataRoots.size() + " location(s)");
        }

        ResolvedScan baseResolved = resolveSide(baseScan, formats.get(0), dataRoots.get(0), version, asOf);

        List<JoinExecution.Step> steps = new ArrayList<>();
        for (int i = 0; i < chain.size(); i++) {
            Join step = chain.get(i);
            TableScan newTableScan = (TableScan) step.right();
            ResolvedScan resolved = resolveSide(newTableScan, formats.get(i + 1), dataRoots.get(i + 1), version, asOf);

            List<String> leftKeys = new ArrayList<>();
            List<String> rightKeys = new ArrayList<>();
            for (JoinKey key : step.keys()) {
                leftKeys.add(key.leftColumn());
                rightKeys.add(key.rightColumn());
            }
            steps.add(new JoinExecution.Step(step.type(), resolved, leftKeys, rightKeys));
        }

        Aggregate aggregate = findAggregate(optimizedPlan);
        Sort sort = findSort(optimizedPlan);
        Limit limit = findLimit(optimizedPlan);

        return new JoinExecution(baseResolved, steps, aggregate, sort, limit);
    }

    /**
     * Unwinds a left-deep {@link Join} chain into first-to-last order —
     * {@code outermost} is the chain's LAST join (its {@code left()} is
     * either another {@code Join}, for an earlier step, or the base
     * {@link TableScan}, if this is the chain's only join).
     */
    private static List<Join> flattenJoinChain(Join outermost) {
        List<Join> reversed = new ArrayList<>();
        LogicalPlan current = outermost;
        while (current instanceof Join j) {
            reversed.add(j);
            current = j.left();
        }
        List<Join> inOrder = new ArrayList<>(reversed);
        java.util.Collections.reverse(inOrder);
        return inOrder;
    }

    private static TableScan baseScanOf(Join outermost) {
        LogicalPlan current = outermost;
        while (current instanceof Join j) {
            current = j.left();
        }
        return (TableScan) current;
    }

    /** One table's independently-resolved scan — shared building block for both {@link #resolve} and {@link #resolveJoin}. */
    private ResolvedScan resolveSide(TableScan scan, TableFormat format, TableLocation dataFileRoot,
                                      Long version, Instant asOf) throws Exception {
        NormalizedSnapshot snapshot = version != null ? format.atVersion(version)
                : asOf != null ? format.atTimestamp(asOf)
                : format.latest();

        NormalizedScanPlan scanPlan = new QueryPlanner().planNormalized(snapshot, scan.predicate(), scan.requestedColumns());

        List<FileScan> files = new ArrayList<>();
        long totalBytes = 0;
        for (DataFileRef file : scanPlan.getFilesToScan()) {
            TableLocation dataFile = dataFileRoot.resolve(file.getPath());
            Predicate perFileResidual =
                    PartitionValueSubstituter.simplify(scanPlan.getResidualPredicate(), file.getPartitionValues());
            files.add(new FileScan(dataFile, perFileResidual, file.getPartitionValues()));
            totalBytes += file.getSizeBytes();
        }

        return new ResolvedScan(files, scan.requestedColumns(), null, null, null,
                snapshot.getVersion(), scanPlan.getTotalFilesInSnapshot(), scanPlan.getFilesSkipped(), totalBytes);
    }

    private TableScan findScan(LogicalPlan plan) {
        if (plan instanceof TableScan scan) {
            return scan;
        }
        if (plan instanceof Filter filter) {
            return findScan(filter.input());
        }
        if (plan instanceof Aggregate aggregate) {
            return findScan(aggregate.input());
        }
        if (plan instanceof Sort sort) {
            return findScan(sort.input());
        }
        if (plan instanceof Limit limit) {
            return findScan(limit.input());
        }
        throw new IllegalStateException("No TableScan found in plan: " + plan);
    }

    /**
     * SQL engine Phase 3, increment 2: generalized from Project-only to
     * recurse through Limit/Sort/Aggregate/Filter too, since a Join may
     * now be wrapped in any combination of those (join + GROUP BY/
     * aggregates/ORDER BY/LIMIT).
     */
    private Join findJoin(LogicalPlan plan) {
        if (plan instanceof Join join) {
            return join;
        }
        if (plan instanceof Project project) {
            return findJoin(project.input());
        }
        if (plan instanceof Limit limit) {
            return findJoin(limit.input());
        }
        if (plan instanceof Sort sort) {
            return findJoin(sort.input());
        }
        if (plan instanceof Aggregate aggregate) {
            return findJoin(aggregate.input());
        }
        if (plan instanceof Filter filter) {
            return findJoin(filter.input());
        }
        throw new IllegalStateException("No Join found in plan: " + plan);
    }

    private Aggregate findAggregate(LogicalPlan plan) {
        if (plan instanceof Aggregate aggregate) {
            return aggregate;
        }
        if (plan instanceof Project project) {
            return findAggregate(project.input());
        }
        if (plan instanceof Filter filter) {
            return findAggregate(filter.input());
        }
        if (plan instanceof Sort sort) {
            return findAggregate(sort.input());
        }
        if (plan instanceof Limit limit) {
            return findAggregate(limit.input());
        }
        return null;
    }

    private Sort findSort(LogicalPlan plan) {
        if (plan instanceof Sort sort) {
            return sort;
        }
        if (plan instanceof Project project) {
            return findSort(project.input());
        }
        if (plan instanceof Filter filter) {
            return findSort(filter.input());
        }
        if (plan instanceof Aggregate aggregate) {
            return findSort(aggregate.input());
        }
        if (plan instanceof Limit limit) {
            return findSort(limit.input());
        }
        return null;
    }

    private Limit findLimit(LogicalPlan plan) {
        if (plan instanceof Limit limit) {
            return limit;
        }
        if (plan instanceof Project project) {
            return findLimit(project.input());
        }
        if (plan instanceof Filter filter) {
            return findLimit(filter.input());
        }
        if (plan instanceof Aggregate aggregate) {
            return findLimit(aggregate.input());
        }
        if (plan instanceof Sort sort) {
            return findLimit(sort.input());
        }
        return null;
    }
}
