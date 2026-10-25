package com.labuk.labukdb.core.planner;

import com.labuk.labukdb.core.format.DataFileRef;

import java.util.List;

/**
 * {@link ScanPlan}'s format-agnostic counterpart — same shape, {@link
 * DataFileRef} instead of {@code AddFile}, produced by {@link
 * QueryPlanner#planNormalized} for any {@code TableFormat} (today: only
 * Iceberg's CLI/Flight path uses this; Delta's own path stays on {@link
 * ScanPlan} unchanged, see {@code QueryPlanner}'s javadoc for why).
 */
public final class NormalizedScanPlan {

    private final List<DataFileRef> filesToScan;
    private final List<String> projectedColumns;
    private final Predicate residualPredicate;
    private final int totalFilesInSnapshot;

    public NormalizedScanPlan(List<DataFileRef> filesToScan, List<String> projectedColumns,
                               Predicate residualPredicate, int totalFilesInSnapshot) {
        this.filesToScan = List.copyOf(filesToScan);
        this.projectedColumns = projectedColumns == null ? List.of() : List.copyOf(projectedColumns);
        this.residualPredicate = residualPredicate;
        this.totalFilesInSnapshot = totalFilesInSnapshot;
    }

    public List<DataFileRef> getFilesToScan() {
        return filesToScan;
    }

    /** Empty list means "all columns" — decode/ layer treats it that way. */
    public List<String> getProjectedColumns() {
        return projectedColumns;
    }

    public Predicate getResidualPredicate() {
        return residualPredicate;
    }

    public int getTotalFilesInSnapshot() {
        return totalFilesInSnapshot;
    }

    public int getFilesSkipped() {
        return totalFilesInSnapshot - filesToScan.size();
    }

    @Override
    public String toString() {
        return "NormalizedScanPlan{scanning=" + filesToScan.size() + "/" + totalFilesInSnapshot
                + " files, skipped=" + getFilesSkipped() + "}";
    }
}
