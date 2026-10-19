package com.labuk.labukdb.core.planner;

import com.labuk.labukdb.core.log.action.AddFile;

import java.util.List;

/**
 * What a query actually needs to scan, after Layer 2's file-level pruning.
 * Row-group and page-level pruning happen one level down, per-file, in
 * {@code decode/} — this plan only decides which files are worth opening
 * at all.
 */
public final class ScanPlan {

    private final List<AddFile> filesToScan;
    private final List<String> projectedColumns;
    private final Predicate residualPredicate;
    private final int totalFilesInSnapshot;

    public ScanPlan(List<AddFile> filesToScan, List<String> projectedColumns,
                     Predicate residualPredicate, int totalFilesInSnapshot) {
        this.filesToScan = List.copyOf(filesToScan);
        this.projectedColumns = projectedColumns == null ? List.of() : List.copyOf(projectedColumns);
        this.residualPredicate = residualPredicate;
        this.totalFilesInSnapshot = totalFilesInSnapshot;
    }

    public List<AddFile> getFilesToScan() {
        return filesToScan;
    }

    /** Empty list means "all columns" — decode/ layer treats it that way. */
    public List<String> getProjectedColumns() {
        return projectedColumns;
    }

    /**
     * The predicate to re-check per row during decode. Even after file- and
     * row-group-level pruning, a surviving file/row-group can still contain
     * non-matching rows — pruning only rules out whole ranges, it doesn't
     * guarantee every remaining row matches. This is what the decode layer's
     * residual filter evaluates.
     */
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
        return "ScanPlan{scanning=" + filesToScan.size() + "/" + totalFilesInSnapshot
                + " files, skipped=" + getFilesSkipped() + "}";
    }
}
