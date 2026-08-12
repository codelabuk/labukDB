package com.labuk.deltaflight.log;

import com.labuk.deltaflight.log.action.AddFile;
import com.labuk.deltaflight.log.action.MetadataAction;
import com.labuk.deltaflight.log.action.ProtocolAction;
import com.labuk.deltaflight.log.action.SingleAction;
import com.labuk.deltaflight.storage.TableLocation;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Resolves a Delta table to a {@link Snapshot} - the live file set and
 * effective schema as of a given version, timestamp, or "latest."
 *
 * <h2>Isolation</h2>
 * A query resolves its snapshot exactly once, via one of the
 * {@code atVersion}/{@code atTimestamp}/{@code latest} calls, and scans
 * against the returned {@link Snapshot} for its whole execution -
 * {@code Snapshot} defensively copies the file set at construction time,
 * so nothing this manager does afterward can retroactively change it.
 *
 * <h2>Caching model</h2>
 * One running cache of "active files as of the highest version replayed
 * so far." Moving forward (new commits landing) applies only the delta.
 * Moving backward (an older version than what's cached) triggers a full
 * reset and replay from a checkpoint or version 0 - not incremental,
 * since a remove action isn't reversible without the removed file's
 * original AddFile, which isn't retained.
 *
 * <h2>Checkpoint support</h2>
 * On a cold start (cache reset to -1), this looks for the highest
 * single-part checkpoint at or below the target version and loads it as
 * the baseline instead of always replaying JSON from version 0 - see
 * {@link CheckpointReader} for the real complexity here (nested Parquet
 * schema, hand-parsed, unverified against a real checkpoint file). If no
 * usable checkpoint exists, behavior is unchanged from before checkpoints
 * existed: replay every JSON commit from 0.
 *
 * <h2>Storage</h2>
 * Two constructors: the original {@code java.nio.file}-backed one
 * (unchanged behavior), and a new Hadoop {@code FileSystem}-backed one
 * for S3/MinIO/ADLS - see {@link LogStorage}'s javadoc for how that
 * dispatches.
 *
 * <h2>Thread safety</h2>
 * All public methods synchronized on this instance.
 */
public class SnapshotManager {

    private final LogStorage storage;
    private final CommitLogReader commitLogReader = new CommitLogReader();
    private final CheckpointReader checkpointReader = new CheckpointReader();

    private long cachedVersion = -1;
    private final Map<String, AddFile> cachedActiveFiles = new ConcurrentHashMap<>();
    private MetadataAction cachedMetadata;
    private ProtocolAction cachedProtocol;

    /** Original constructor - local filesystem, behavior unchanged from before this abstraction existed. */
    public SnapshotManager(Path tableRoot) {
        this.storage = new LocalNioLogStorage(tableRoot.resolve("_delta_log"));
    }

    /**
     * New: Hadoop {@code FileSystem}-backed, for S3/MinIO/ADLS (given the
     * right connector jar and {@code Configuration} entries - endpoint,
     * credentials - which this constructor does not set up itself).
     */
    public SnapshotManager(org.apache.hadoop.fs.Path tableRoot, Configuration conf) throws IOException {
        this.storage = new HadoopLogStorage(new org.apache.hadoop.fs.Path(tableRoot, "_delta_log"), conf);
    }

    /**
     * Dispatches on the location itself - local or remote - so callers
     * don't have to pick a constructor based on inspecting a URI scheme
     * themselves. This is the one that {@code TableCache} and, through it,
     * {@code DeltaFlightProducer} actually use.
     */
    public static SnapshotManager forLocation(TableLocation location) throws IOException {
        return location.isRemote()
                ? new SnapshotManager(location.hadoopPath(), location.hadoopConf())
                : new SnapshotManager(location.localPath());
    }

    public synchronized Snapshot latest() {
        List<Long> versions = listCommitVersions();
        if (versions.isEmpty()) {
            throw new IllegalStateException("No commits found under " + storage.describe());
        }
        return atVersion(versions.get(versions.size() - 1));
    }

    public synchronized Snapshot atVersion(long targetVersion) {
        if (targetVersion < 0) {
            throw new IllegalArgumentException("version must be >= 0, got " + targetVersion);
        }

        List<Long> versions = listCommitVersions();
        if (versions.isEmpty()) {
            throw new IllegalStateException("No commits found under " + storage.describe());
        }
        long maxAvailable = versions.get(versions.size() - 1);
        if (targetVersion > maxAvailable) {
            throw new IllegalArgumentException(
                    "Requested version " + targetVersion + " has not been committed yet; latest is " + maxAvailable);
        }

        if (targetVersion < cachedVersion) {
            resetCache();
        }
        if (cachedVersion == -1) {
            loadFromCheckpointIfAvailable(targetVersion);
        }

        for (long v = cachedVersion + 1; v <= targetVersion; v++) {
            applyCommit(v);
        }
        cachedVersion = targetVersion;

        return buildSnapshot(targetVersion);
    }

    public synchronized Snapshot atTimestamp(Instant target) {
        long targetMillis = target.toEpochMilli();
        List<Long> versions = listCommitVersions();
        if (versions.isEmpty()) {
            throw new IllegalStateException("No commits found under " + storage.describe());
        }

        Long resolved = null;
        for (long v : versions) {
            if (timestampOfCommit(v) <= targetMillis) {
                resolved = v;
            } else {
                break;
            }
        }
        if (resolved == null) {
            throw new IllegalArgumentException(
                    "No table version exists at or before " + target + " - the table's first commit is later than that");
        }
        return atVersion(resolved);
    }

    /**
     * Finds the highest checkpoint at or below {@code upToVersion} and loads
     * it as the cache baseline. Supports both single-part and multi-part
     * checkpoints.
     *
     * Multi-part checkpoints (e.g.
     * {@code 00000000000000000010.checkpoint.0000000001.0000000003.parquet},
     * {@code 00000000000000000010.checkpoint.0000000002.0000000003.parquet},
     * {@code 00000000000000000010.checkpoint.0000000003.0000000003.parquet})
     * are read in part-number order and merged — each part is a subset of the
     * full live file set, and together they reconstruct the same state a
     * single-part checkpoint would. Missing a part would silently produce an
     * incomplete live set, so this throws if any part is missing rather than
     * silently reading a partial checkpoint.
     */
    private void loadFromCheckpointIfAvailable(long upToVersion) {
        List<String> allCheckpointFiles;
        try {
            allCheckpointFiles = storage.listCheckpointFileNames();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to list checkpoint files in " + storage.describe(), e);
        }

        Optional<Long> checkpointVersion = allCheckpointFiles.stream()
                .map(CommitFileNames::parseCheckpointVersion)
                .filter(v -> v <= upToVersion)
                .max(Long::compareTo);

        if (checkpointVersion.isEmpty()) {
            return;
        }

        long version = checkpointVersion.get();
        List<String> parts = allCheckpointFiles.stream()
                .filter(f -> CommitFileNames.parseCheckpointVersion(f) == version)
                .sorted()
                .collect(java.util.stream.Collectors.toList());

        boolean isMultiPart = parts.size() > 1
                || (parts.size() == 1 && CommitFileNames.isMultiPartCheckpoint(parts.get(0)));

        if (isMultiPart) {
            loadMultiPartCheckpoint(version, parts);
        } else {
            loadSinglePartCheckpoint(parts.get(0));
        }
        cachedVersion = version;
    }

    private void loadSinglePartCheckpoint(String fileName) {
        List<SingleAction> actions = checkpointReader.read(storage, fileName);
        applyCheckpointActions(actions);
    }

    private void loadMultiPartCheckpoint(long version, List<String> parts) {
        // Verify all parts are present before reading any — a partial read
        // would silently produce an incomplete live set, which is worse than
        // a clear failure.
        if (!parts.isEmpty() && CommitFileNames.isMultiPartCheckpoint(parts.get(0))) {
            int expectedTotal = CommitFileNames.parseCheckpointTotalParts(parts.get(0));
            if (parts.size() != expectedTotal) {
                throw new IllegalStateException(
                        "Multi-part checkpoint at version " + version + " is incomplete: "
                                + "found " + parts.size() + " of " + expectedTotal + " expected parts. "
                                + "The checkpoint may have been written partially or some files may have "
                                + "been cleaned up prematurely. Cannot safely reconstruct the live file set.");
            }
        }
        for (String part : parts) {
            List<SingleAction> actions = checkpointReader.read(storage, part);
            applyCheckpointActions(actions);
        }
    }

    private void applyCheckpointActions(List<SingleAction> actions) {
        for (SingleAction action : actions) {
            if (action.getAdd() != null) {
                cachedActiveFiles.put(action.getAdd().getPath(), action.getAdd());
            }
            if (action.getMetaData() != null) {
                cachedMetadata = action.getMetaData();
            }
            if (action.getProtocol() != null) {
                cachedProtocol = action.getProtocol();
            }
        }
    }

    private void applyCommit(long version) {
        String fileName = CommitFileNames.jsonFileName(version);
        List<SingleAction> actions;
        try (BufferedReader reader = storage.openReader(fileName)) {
            actions = commitLogReader.readFrom(reader, storage.describe() + "/" + fileName);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read commit " + fileName + " from " + storage.describe(), e);
        }

        for (SingleAction action : actions) {
            if (action.getAdd() != null) {
                cachedActiveFiles.put(action.getAdd().getPath(), action.getAdd());
            }
            if (action.getRemove() != null) {
                cachedActiveFiles.remove(action.getRemove().getPath());
            }
            if (action.getMetaData() != null) {
                cachedMetadata = action.getMetaData();
            }
            if (action.getProtocol() != null) {
                cachedProtocol = action.getProtocol();
            }
        }
    }

    private long timestampOfCommit(long version) {
        String fileName = CommitFileNames.jsonFileName(version);
        List<SingleAction> actions;
        try (BufferedReader reader = storage.openReader(fileName)) {
            actions = commitLogReader.readFrom(reader, storage.describe() + "/" + fileName);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read commit " + fileName + " from " + storage.describe(), e);
        }
        for (SingleAction action : actions) {
            if (action.getCommitInfo() != null && action.getCommitInfo().getTimestamp() != null) {
                return action.getCommitInfo().getTimestamp();
            }
        }
        try {
            return storage.getModificationTime(fileName);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read modification time for " + fileName, e);
        }
    }

    private void resetCache() {
        cachedVersion = -1;
        cachedActiveFiles.clear();
        cachedMetadata = null;
        cachedProtocol = null;
    }

    private Snapshot buildSnapshot(long version) {
        return new Snapshot(version, cachedActiveFiles, cachedMetadata, cachedProtocol);
    }

    private List<Long> listCommitVersions() {
        try {
            return storage.listCommitFileNames().stream()
                    .map(CommitFileNames::parseVersion)
                    .sorted()
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to list commit files in " + storage.describe(), e);
        }
    }
}
