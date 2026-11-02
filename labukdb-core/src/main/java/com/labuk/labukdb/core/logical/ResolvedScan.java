package com.labuk.labukdb.core.logical;

import com.labuk.labukdb.core.execution.FileScan;

import java.util.List;

/**
 * What {@link LogicalPlanExecutor#resolve} produces: the files to scan and
 * everything the caller needs to either stream-and-print per file or hand
 * to {@code CombinedQueryExecutor}, plus the metadata needed to print the
 * same "Table: .../Files: ..." summary {@code QueryCommand} already prints
 * today. Metadata only — this does not execute aggregation or printing.
 */
public record ResolvedScan(List<FileScan> files, List<String> columns,
                            Aggregate aggregate, Sort sort, Limit limit,
                            long snapshotVersion, int totalFilesInSnapshot, int filesSkipped) {

    /**
     * True when {@code CombinedQueryExecutor} is genuinely required —
     * {@code Sort} or {@code Aggregate} present, either of which needs to
     * see every surviving row before producing a correct result. A bare
     * {@code Limit} with neither (see {@link #hasLimitOnly()}) does not —
     * SQL engine Phase 2's whole point.
     */
    public boolean needsCombine() {
        return aggregate != null || sort != null;
    }

    /**
     * True for e.g. {@code "SELECT x FROM t LIMIT 3"} — a cap with no
     * ordering/grouping requirement, which can stop scanning as soon as
     * enough rows are found instead of materializing every surviving file
     * first the way {@code CombinedQueryExecutor} would.
     */
    public boolean hasLimitOnly() {
        return limit != null && !needsCombine();
    }
}
