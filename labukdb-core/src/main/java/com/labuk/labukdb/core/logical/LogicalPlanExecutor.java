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
 * Returns metadata only — does not execute aggregation or printing
 * itself; the terminal execution (stream-and-print vs. combine-and-print)
 * stays the caller's job, preserving the exact memory profile each case
 * already has.
 */
public final class LogicalPlanExecutor {

    public ResolvedScan resolve(LogicalPlan optimizedPlan, TableFormat format, TableLocation dataFileRoot,
                                 Long version, Instant asOf) throws Exception {
        TableScan scan = findScan(optimizedPlan);
        Aggregate aggregate = findAggregate(optimizedPlan);
        Sort sort = findSort(optimizedPlan);
        Limit limit = findLimit(optimizedPlan);

        NormalizedSnapshot snapshot = version != null ? format.atVersion(version)
                : asOf != null ? format.atTimestamp(asOf)
                : format.latest();

        NormalizedScanPlan scanPlan = new QueryPlanner().planNormalized(snapshot, scan.predicate(), scan.requestedColumns());

        List<FileScan> files = new ArrayList<>();
        for (DataFileRef file : scanPlan.getFilesToScan()) {
            TableLocation dataFile = dataFileRoot.resolve(file.getPath());
            Predicate perFileResidual =
                    PartitionValueSubstituter.simplify(scanPlan.getResidualPredicate(), file.getPartitionValues());
            files.add(new FileScan(dataFile, perFileResidual, file.getPartitionValues()));
        }

        return new ResolvedScan(files, scan.requestedColumns(), aggregate, sort, limit,
                snapshot.getVersion(), scanPlan.getTotalFilesInSnapshot(), scanPlan.getFilesSkipped());
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

    private Aggregate findAggregate(LogicalPlan plan) {
        if (plan instanceof Aggregate aggregate) {
            return aggregate;
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
