package com.labuk.deltaflight.log;

import com.labuk.deltaflight.log.action.AddFile;
import com.labuk.deltaflight.log.action.MetadataAction;
import com.labuk.deltaflight.log.action.ProtocolAction;
import com.labuk.deltaflight.log.action.SingleAction;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Resolves a Delta table (a directory containing {@code _delta_log/}) to a
 * {@link Snapshot} — the live file set and effective schema as of a given
 * version, timestamp, or "latest."
 *
 * <h2>Isolation</h2>
 * A query resolves its snapshot exactly once, at the start, via one of the
 * {@code atVersion}/{@code atTimestamp}/{@code latest} calls, and scans
 * against the returned {@link Snapshot} object for its whole execution.
 * SnapshotManager's internal cache can keep advancing underneath — new
 * commits arriving from a streaming writer, other queries calling
 * {@code latest()} — none of that touches a Snapshot a caller already holds,
 * because Snapshot defensively copies the file set at construction time.
 *
 * <h2>Caching model</h2>
 * Replaying every commit from version 0 on every call would make a
 * frequently-queried streaming table (many small commits) expensive to
 * serve. Instead this class keeps one running cache of "active files as of
 * the highest version I've replayed so far" and applies only the delta when
 * asked for a version it hasn't reached yet. Two consequences:
 * <ul>
 *   <li>Moving forward (the common case — new commits keep landing) is
 *       cheap: apply just the new commits' add/remove actions.</li>
 *   <li>Moving backward (asking for an older version than what's cached)
 *       is NOT incremental — a remove action isn't reversible without
 *       knowing what the removed file's AddFile looked like, and we don't
 *       retain that history. This triggers a full replay from version 0.
 *       Time-travel to old versions is expected to be rarer and less
 *       latency-sensitive than "give me the current state," so this
 *       asymmetry is a deliberate tradeoff, not an oversight.</li>
 * </ul>
 *
 * <h2>What's not here yet</h2>
 * No checkpoint (compacted {@code .checkpoint.parquet}) support — every
 * resolution replays JSON commits from version 0 forward on a cache miss.
 * Fine for now; will matter once a table accumulates thousands of commits.
 *
 * <h2>Thread safety</h2>
 * All public methods are synchronized on this instance. One SnapshotManager
 * serializes its own cache updates; it does not serialize access to the
 * Snapshot objects it hands out, which don't need it since they're
 * immutable.
 */
public class SnapshotManager {

    private final Path logDir;
    private final CommitLogReader commitLogReader = new CommitLogReader();

    // Running cache: state as of `cachedVersion`. -1 means nothing replayed yet.
    private long cachedVersion = -1;
    private final Map<String, AddFile> cachedActiveFiles = new ConcurrentHashMap<>();
    private MetadataAction cachedMetadata;
    private ProtocolAction cachedProtocol;

    /**
     * @param tableRoot the table's root directory (the one containing
     *                  {@code _delta_log/}), not the log directory itself
     */
    public SnapshotManager(Path tableRoot) {
        this.logDir = tableRoot.resolve("_delta_log");
    }

    /** Resolves to the highest committed version. */
    public synchronized Snapshot latest() {
        List<Long> versions = listCommitVersions();
        if (versions.isEmpty()) {
            throw new IllegalStateException("No commits found under " + logDir);
        }
        return atVersion(versions.get(versions.size() - 1));
    }

    /** Resolves to an exact version. Throws if that version hasn't been committed yet. */
    public synchronized Snapshot atVersion(long targetVersion) {
        if (targetVersion < 0) {
            throw new IllegalArgumentException("version must be >= 0, got " + targetVersion);
        }

        List<Long> versions = listCommitVersions();
        if (versions.isEmpty()) {
            throw new IllegalStateException("No commits found under " + logDir);
        }
        long maxAvailable = versions.get(versions.size() - 1);
        if (targetVersion > maxAvailable) {
            throw new IllegalArgumentException(
                    "Requested version " + targetVersion + " has not been committed yet; latest is " + maxAvailable);
        }

        if (targetVersion < cachedVersion) {
            resetCache();
        }

        for (long v = cachedVersion + 1; v <= targetVersion; v++) {
            applyCommit(v);
        }
        cachedVersion = targetVersion;

        return buildSnapshot(targetVersion);
    }

    /**
     * Resolves to the latest version whose commit timestamp is at or before
     * {@code target}. Throws if every commit is later than the requested
     * timestamp (i.e. the table didn't exist yet at that point in time).
     */
    public synchronized Snapshot atTimestamp(Instant target) {
        long targetMillis = target.toEpochMilli();
        List<Long> versions = listCommitVersions();
        if (versions.isEmpty()) {
            throw new IllegalStateException("No commits found under " + logDir);
        }

        Long resolved = null;
        for (long v : versions) {
            long commitTimestamp = timestampOfCommit(v);
            if (commitTimestamp <= targetMillis) {
                resolved = v;
            } else {
                break; // versions are in ascending order; no later commit can qualify
            }
        }

        if (resolved == null) {
            throw new IllegalArgumentException(
                    "No table version exists at or before " + target + " — the table's first commit is later than that");
        }

        return atVersion(resolved);
    }

    private void applyCommit(long version) {
        Path commitFile = logDir.resolve(CommitFileNames.jsonFileName(version));
        List<SingleAction> actions = commitLogReader.read(commitFile);
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
        Path commitFile = logDir.resolve(CommitFileNames.jsonFileName(version));
        List<SingleAction> actions = commitLogReader.read(commitFile);
        for (SingleAction action : actions) {
            if (action.getCommitInfo() != null && action.getCommitInfo().getTimestamp() != null) {
                return action.getCommitInfo().getTimestamp();
            }
        }
        // Fallback for commits that, unusually, carry no commitInfo timestamp.
        try {
            return Files.getLastModifiedTime(commitFile).toMillis();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read modification time for " + commitFile, e);
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
        try (Stream<Path> stream = Files.list(logDir)) {
            return stream.filter(CommitFileNames::isCommitJson)
                    .map(CommitFileNames::parseVersion)
                    .sorted()
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to list commit files in " + logDir, e);
        }
    }
}
