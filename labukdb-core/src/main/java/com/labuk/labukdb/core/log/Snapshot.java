package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.log.action.AddFile;
import com.labuk.labukdb.core.log.action.MetadataAction;
import com.labuk.labukdb.core.log.action.ProtocolAction;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The table as of one specific version: the exact set of live files, plus
 * the schema/partition columns in effect at that version.
 *
 * This is the object a query scans against. It's built once and never
 * mutated — that's what gives a query snapshot isolation. SnapshotManager's
 * internal cache keeps moving forward as new commits arrive, but a
 * Snapshot handed to a query is a defensive copy: later commits landing in
 * SnapshotManager cannot retroactively change a file set a query already
 * started scanning.
 */
public final class Snapshot {

    private final long version;
    private final Map<String, AddFile> activeFilesByPath;
    private final MetadataAction metadata;
    private final ProtocolAction protocol;

    Snapshot(long version, Map<String, AddFile> activeFilesByPath, MetadataAction metadata, ProtocolAction protocol) {
        this.version = version;
        // Defensive copy — see class javadoc. The caller's map keeps changing
        // after this constructor returns; ours must not.
        this.activeFilesByPath = Collections.unmodifiableMap(new LinkedHashMap<>(activeFilesByPath));
        this.metadata = metadata;
        this.protocol = protocol;
    }

    public long getVersion() {
        return version;
    }

    public Collection<AddFile> getActiveFiles() {
        return activeFilesByPath.values();
    }

    public int getActiveFileCount() {
        return activeFilesByPath.size();
    }

    public MetadataAction getMetadata() {
        return metadata;
    }

    public ProtocolAction getProtocol() {
        return protocol;
    }

    @Override
    public String toString() {
        return "Snapshot{version=" + version + ", activeFiles=" + activeFilesByPath.size() + "}";
    }
}
