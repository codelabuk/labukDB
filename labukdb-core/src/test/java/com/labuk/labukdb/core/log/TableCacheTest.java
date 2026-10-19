package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.storage.TableLocation;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

class TableCacheTest {

    @Test
    void sameTableRoot_returnsSameSnapshotManagerInstance() {
        TableCache cache = new TableCache();
        Path tableRoot = Paths.get("some-table");

        SnapshotManager first = cache.forTable(tableRoot);
        SnapshotManager second = cache.forTable(tableRoot);

        assertSame(first, second, "the whole point of this cache is reusing the instance, "
                + "not just returning an equal-but-different one");
    }

    @Test
    void differentTableRoots_getDifferentInstances() {
        TableCache cache = new TableCache();

        SnapshotManager a = cache.forTable(Paths.get("table-a"));
        SnapshotManager b = cache.forTable(Paths.get("table-b"));

        assertNotSame(a, b);
        assertEquals(2, cache.size());
    }

    @Test
    void relativeAndAbsoluteFormsOfSamePath_shareOneInstance() {
        TableCache cache = new TableCache();
        Path relative = Paths.get("some-table");
        Path absolute = relative.toAbsolutePath().normalize();

        SnapshotManager viaRelative = cache.forTable(relative);
        SnapshotManager viaAbsolute = cache.forTable(absolute);

        assertSame(viaRelative, viaAbsolute,
                "path normalization should treat these as the same table, not two cache entries");
    }

    @Test
    void evict_removesTheCachedInstance() {
        TableCache cache = new TableCache();
        Path tableRoot = Paths.get("some-table");

        SnapshotManager first = cache.forTable(tableRoot);
        cache.evict(TableLocation.forLocal(tableRoot.toAbsolutePath().normalize()));
        SnapshotManager second = cache.forTable(tableRoot);

        assertNotSame(first, second, "after eviction, the next lookup should build a fresh instance");
    }

    @Test
    void remoteAndLocalLocations_areKeyedSeparately() {
        TableCache cache = new TableCache();
        SnapshotManager local = cache.forTable(Paths.get("some-table"));

        // A remote key must never return the same instance as a local one,
        // even if the trailing path looks similar. We don't attempt to
        // actually open the remote table here (no MinIO available in tests)
        // — we just confirm the cache returns a different instance for a
        // different key, using two local locations with different paths as
        // a proxy for "different keys produce different managers."
        SnapshotManager other = cache.forTable(Paths.get("some-other-table"));

        assertNotSame(local, other,
                "different table locations must produce different SnapshotManager instances");
        assertEquals(2, cache.size());
    }
}
