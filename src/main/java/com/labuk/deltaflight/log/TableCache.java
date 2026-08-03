package com.labuk.deltaflight.log;

import com.labuk.deltaflight.storage.TableLocation;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reuses one {@link SnapshotManager} per table location across requests,
 * rather than constructing a fresh one on every call.
 *
 * <b>Why this matters, concretely:</b> {@code SnapshotManager}'s
 * incremental forward cache is designed for a frequently-queried,
 * frequently-updated (streaming) table, where re-replaying the whole
 * commit log on every query would be wasteful. That cache only helps if
 * the same instance survives across queries - without this class, ten
 * queries against the same table a second apart meant ten full replays,
 * not nine cache hits.
 *
 * Keyed by the location's raw string (local absolute path or remote URI),
 * so a local table and a remote one never collide, and the same remote
 * URI reuses one manager the same way a local path does.
 *
 * <b>Thread safety:</b> {@code SnapshotManager}'s public methods are
 * synchronized on the instance, so sharing an instance across concurrent
 * gRPC request threads is safe.
 */
public final class TableCache {

    private final Map<String, SnapshotManager> managers = new ConcurrentHashMap<>();

    /** Dispatches local vs. remote via {@link SnapshotManager#forLocation}. */
    public SnapshotManager forTable(TableLocation location) {
        String key = key(location);
        SnapshotManager existing = managers.get(key);
        if (existing != null) {
            return existing;
        }
        // Not using computeIfAbsent here deliberately: if the SnapshotManager
        // constructor throws (e.g. no MinIO reachable), computeIfAbsent leaves
        // a null mapped to the key in some JDK versions, meaning a subsequent
        // call for the same location would find the dead entry and fail again
        // without retrying. Build first, then put only if it succeeds.
        try {
            SnapshotManager manager = SnapshotManager.forLocation(location);
            SnapshotManager raced = managers.putIfAbsent(key, manager);
            return raced != null ? raced : manager;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open table at " + location, e);
        }
    }

    /** Convenience for local-only callers - same behavior as before TableLocation existed. */
    public SnapshotManager forTable(Path tableRoot) {
        return forTable(TableLocation.forLocal(tableRoot.toAbsolutePath().normalize()));
    }

    /** Not currently wired to anything - a seam for an eventual cache-invalidation admin action. */
    public void evict(TableLocation location) {
        managers.remove(key(location));
    }

    public int size() {
        return managers.size();
    }

    private static String key(TableLocation location) {
        return location.isRemote()
                ? location.raw()
                : location.localPath().toAbsolutePath().normalize().toString();
    }
}
