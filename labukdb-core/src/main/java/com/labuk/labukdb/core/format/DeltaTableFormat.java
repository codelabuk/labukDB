package com.labuk.labukdb.core.format;

import com.labuk.labukdb.core.log.Snapshot;
import com.labuk.labukdb.core.log.SnapshotManager;
import com.labuk.labukdb.core.log.action.AddFile;
import com.labuk.labukdb.core.log.action.MetadataAction;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link TableFormat} for Delta — pure delegation to the existing
 * {@code log.SnapshotManager}, translating its Delta-shaped {@link Snapshot}
 * into the format-agnostic {@link NormalizedSnapshot}. No new resolution
 * logic lives here; see {@link TableFormat}'s javadoc for this class's
 * current scope (tested standalone, not yet wired into
 * {@code planner}/{@code flight}/{@code cli}).
 */
public final class DeltaTableFormat implements TableFormat {

    private final SnapshotManager snapshotManager;

    public DeltaTableFormat(Path tableRoot) {
        this.snapshotManager = new SnapshotManager(tableRoot);
    }

    @Override
    public NormalizedSnapshot latest() {
        return normalize(snapshotManager.latest());
    }

    @Override
    public NormalizedSnapshot atVersion(long version) {
        return normalize(snapshotManager.atVersion(version));
    }

    @Override
    public NormalizedSnapshot atTimestamp(Instant timestamp) {
        return normalize(snapshotManager.atTimestamp(timestamp));
    }

    private NormalizedSnapshot normalize(Snapshot snapshot) {
        MetadataAction metadata = snapshot.getMetadata();
        String schemaString = metadata == null ? null : metadata.getSchemaString();
        List<String> partitionColumns = metadata == null ? List.of() : metadata.getPartitionColumns();

        List<DataFileRef> files = snapshot.getActiveFiles().stream()
                .map(this::toDataFileRef)
                .collect(Collectors.toList());

        return new NormalizedSnapshot(snapshot.getVersion(), schemaString, partitionColumns, files);
    }

    private DataFileRef toDataFileRef(AddFile addFile) {
        return new DataFileRef(
                addFile.getPath(),
                addFile.getPartitionValues(),
                addFile.getSize(),
                addFile.getStats());
    }
}
