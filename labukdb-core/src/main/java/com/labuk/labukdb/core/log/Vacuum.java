package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.log.action.AddFile;
import com.labuk.labukdb.core.log.action.RemoveFile;
import com.labuk.labukdb.core.log.action.SingleAction;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Physically deletes Parquet files that {@code remove} actions already
 * dropped from the live set, once they're older than the retention
 * period — the actual space-reclamation step {@link Compactor} and plain
 * {@code remove} actions both only logically defer to. A {@code remove}
 * action never deletes bytes by itself (see {@code RemoveFile}'s
 * javadoc) — that's what makes time travel to an earlier version work;
 * this class is what eventually ends that guarantee for versions old
 * enough that nothing should still need them.
 *
 * <b>Scans the full commit history</b>, not just the latest snapshot —
 * deliberately bypasses {@link SnapshotManager}'s incremental-replay
 * cache, since the whole point is finding <em>old</em> {@code remove}
 * actions a resolved snapshot wouldn't carry at all.
 *
 * <b>The one safety invariant that actually matters:</b> a file still in
 * the <em>current</em> active set is never deleted, regardless of what
 * any historical commit claims — checked explicitly, not assumed from
 * the retention window alone.
 */
public final class Vacuum {

    /** Matches real Delta Lake's own default retention — not invented here. */
    public static final Duration DEFAULT_RETENTION = Duration.ofDays(7);

    /** @return the relative paths actually deleted. */
    public List<String> vacuum(Path tableRoot) throws IOException {
        return vacuum(tableRoot, DEFAULT_RETENTION);
    }

    public List<String> vacuum(Path tableRoot, Duration retentionPeriod) throws IOException {
        Snapshot current = new SnapshotManager(tableRoot).latest();
        Set<String> activePaths = new LinkedHashSet<>();
        for (AddFile file : current.getActiveFiles()) {
            activePaths.add(file.getPath());
        }

        long cutoffMillis = System.currentTimeMillis() - retentionPeriod.toMillis();
        Set<String> staleRemovedPaths = collectStaleRemovedPaths(tableRoot, cutoffMillis);

        List<String> deleted = new ArrayList<>();
        for (String path : staleRemovedPaths) {
            if (activePaths.contains(path)) {
                continue; // safety invariant: never delete a file still in the live set
            }
            if (Files.deleteIfExists(tableRoot.resolve(path))) {
                deleted.add(path);
            }
        }
        return deleted;
    }

    private Set<String> collectStaleRemovedPaths(Path tableRoot, long cutoffMillis) throws IOException {
        LocalNioLogStorage storage = new LocalNioLogStorage(tableRoot.resolve("_delta_log"));
        CommitLogReader commitLogReader = new CommitLogReader();

        Set<String> stalePaths = new LinkedHashSet<>();
        for (String fileName : storage.listCommitFileNames()) {
            try (BufferedReader reader = storage.openReader(fileName)) {
                for (SingleAction action : commitLogReader.readFrom(reader, fileName)) {
                    RemoveFile remove = action.getRemove();
                    if (remove != null && remove.getDeletionTimestamp() != null
                            && remove.getDeletionTimestamp() < cutoffMillis) {
                        stalePaths.add(remove.getPath());
                    }
                }
            }
        }
        return stalePaths;
    }
}
