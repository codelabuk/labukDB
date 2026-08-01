package com.labuk.deltaflight.log;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reuses one {@link SnapshotManager} per table root across requests, rather
 * than constructing a fresh one on every call.
 *
 * <b>Why this matters, concretely:</b> {@code SnapshotManager}'s own
 * javadoc describes an incremental forward cache designed for exactly the
 * case that matters most — a frequently-queried, frequently-updated
 * (streaming) table, where re-replaying the whole commit log from version 0
 * on every query would be wasteful. That cache only helps if the *same*
 * instance survives across queries. Before this class existed, the code
 * calling {@code new SnapshotManager(tableRoot)} once per request threw
 * that benefit away every single time — ten queries against the same table
 * a second apart meant ten full replays from version 0, not nine cache hits
 * and one real replay.
 *
 * <b>Thread safety:</b> {@code SnapshotManager}'s own public methods are
 * already synchronized on the instance, so handing the same instance to
 * concurrent gRPC request threads is safe — this class just makes sure
 * they get the same instance in the first place.
 */
public final class TableCache {

    private final Map<String, SnapshotManager> managers = new ConcurrentHashMap<>();

    public SnapshotManager forTable(Path tableRoot) {
        String key = normalize(tableRoot);
        return managers.computeIfAbsent(key, k -> new SnapshotManager(tableRoot));
    }

    /** Not currently wired to anything — a seam for an eventual cache-invalidation admin action. */
    public void evict(Path tableRoot) {
        managers.remove(normalize(tableRoot));
    }

    public int size() {
        return managers.size();
    }

    private static String normalize(Path tableRoot) {
        return tableRoot.toAbsolutePath().normalize().toString();
    }
}
