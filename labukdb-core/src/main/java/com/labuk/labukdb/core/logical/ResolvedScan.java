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

    public boolean needsCombine() {
        return aggregate != null || sort != null || limit != null;
    }
}
