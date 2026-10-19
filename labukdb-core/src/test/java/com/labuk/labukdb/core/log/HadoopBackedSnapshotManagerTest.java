package com.labuk.labukdb.core.log;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Confirms SnapshotManager's new Hadoop-backed constructor produces the
 * exact same result as the original local one, for the same local
 * directory — this is what's actually verifiable here. It does NOT prove
 * S3/MinIO/ADLS work, since there's no real endpoint to test against in
 * this environment; it proves the abstraction didn't change local
 * behavior, and that Hadoop's own local-filesystem dispatch (what handles
 * a plain path with no URI scheme) works as expected.
 */
class HadoopBackedSnapshotManagerTest {

    private static final Instant V0_TIMESTAMP = Instant.ofEpochMilli(1732000000000L);

    @Test
    void hadoopBackedManager_matchesLocalManager_forSameDirectory(@TempDir Path tempDir) throws Exception {
        Path tableRoot = tempDir.resolve("sample-table");
        Path logDir = tableRoot.resolve("_delta_log");
        Files.createDirectories(logDir);
        Files.copy(fixture("00000000000000000000.json"), logDir.resolve("00000000000000000000.json"));
        Files.copy(fixture("00000000000000000001.json"), logDir.resolve("00000000000000000001.json"));

        SnapshotManager local = new SnapshotManager(tableRoot);
        SnapshotManager hadoopBacked = new SnapshotManager(
                new org.apache.hadoop.fs.Path(tableRoot.toString()), new Configuration());

        Snapshot localLatest = local.latest();
        Snapshot hadoopLatest = hadoopBacked.latest();

        assertEquals(localLatest.getVersion(), hadoopLatest.getVersion());
        assertEquals(localLatest.getActiveFileCount(), hadoopLatest.getActiveFileCount());
        assertEquals(localLatest.getMetadata().getPartitionColumns(), hadoopLatest.getMetadata().getPartitionColumns());

        Snapshot localAtTs = local.atTimestamp(V0_TIMESTAMP);
        Snapshot hadoopAtTs = hadoopBacked.atTimestamp(V0_TIMESTAMP);
        assertEquals(localAtTs.getVersion(), hadoopAtTs.getVersion());
    }

    private Path fixture(String name) throws Exception {
        return Path.of(getClass().getClassLoader().getResource("sample-table/_delta_log/" + name).toURI());
    }
}
