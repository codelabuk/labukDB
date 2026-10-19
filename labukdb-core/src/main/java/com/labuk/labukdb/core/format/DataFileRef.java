package com.labuk.labukdb.core.format;

import java.util.Map;

/**
 * One data file in a table's live set, independent of which table format
 * produced it — the generalized shape {@code log.action.AddFile} would
 * need to become if a second {@link TableFormat} implementation (Iceberg)
 * ever consumes the same downstream code. For now this is only ever
 * constructed by {@link DeltaTableFormat}, translating from {@code AddFile}
 * — nothing else in the engine consumes it yet, deliberately; see
 * {@link TableFormat}'s javadoc for why.
 */
public final class DataFileRef {

    private final String path;
    private final Map<String, String> partitionValues;
    private final long sizeBytes;
    private final String stats;

    public DataFileRef(String path, Map<String, String> partitionValues, long sizeBytes, String stats) {
        this.path = path;
        this.partitionValues = partitionValues;
        this.sizeBytes = sizeBytes;
        this.stats = stats;
    }

    public String getPath() {
        return path;
    }

    public Map<String, String> getPartitionValues() {
        return partitionValues;
    }

    public long getSizeBytes() {
        return sizeBytes;
    }

    /** Raw stats JSON, same shape as {@code AddFile.getStats()} — nullable. */
    public String getStats() {
        return stats;
    }
}
