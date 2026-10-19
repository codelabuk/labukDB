package com.labuk.labukdb.core.format;

import java.util.List;

/**
 * A table's state at one point in time, independent of which table format
 * produced it. Mirrors {@code log.Snapshot} field for field today — the
 * translation in {@link DeltaTableFormat} is pure delegation, not new
 * logic — but kept as its own type rather than reusing {@code Snapshot}
 * directly, since {@code Snapshot} is shaped around Delta's own action
 * model ({@code AddFile}, {@code MetadataAction}) in ways a second format
 * wouldn't share.
 */
public final class NormalizedSnapshot {

    private final long version;
    private final String schemaString;
    private final List<String> partitionColumns;
    private final List<DataFileRef> files;

    public NormalizedSnapshot(long version, String schemaString, List<String> partitionColumns, List<DataFileRef> files) {
        this.version = version;
        this.schemaString = schemaString;
        this.partitionColumns = partitionColumns;
        this.files = files;
    }

    public long getVersion() {
        return version;
    }

    /** Raw schema string, same representation {@code MetadataAction.getSchemaString()} carries today. */
    public String getSchemaString() {
        return schemaString;
    }

    public List<String> getPartitionColumns() {
        return partitionColumns;
    }

    public List<DataFileRef> getFiles() {
        return files;
    }
}
