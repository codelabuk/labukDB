package com.labuk.labukdb.core.log;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class VacuumTest {

    private static final long NOW = System.currentTimeMillis();
    private static final long OLD = NOW - Duration.ofDays(10).toMillis();
    private static final long RECENT = NOW - Duration.ofHours(1).toMillis();

    /**
     * Three files, three outcomes:
     *  - fileOld.parquet:    removed 10 days ago, not active -> deleted.
     *  - fileRecent.parquet: removed 1 hour ago, not active -> kept (within retention).
     *  - fileX.parquet:      removed 10 days ago (stale), but RE-ADDED since
     *                        and currently active -> must be kept regardless
     *                        of the old remove entry -- the actual safety
     *                        invariant this class exists to guarantee.
     */
    private void writeFixtureHistory(Path tableRoot) throws Exception {
        Path logDir = tableRoot.resolve("_delta_log");
        Files.createDirectories(logDir);

        String metaData = "{\"metaData\":{\"id\":\"vacuum-test\",\"format\":{\"provider\":\"parquet\",\"options\":{}},"
                + "\"schemaString\":\"{}\",\"partitionColumns\":[],\"configuration\":{},\"createdTime\":1732000000000}}";

        Files.writeString(logDir.resolve("00000000000000000000.json"), String.join("\n",
                "{\"commitInfo\":{\"timestamp\":1732000000000,\"operation\":\"CREATE TABLE\"}}",
                "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}",
                metaData,
                "{\"add\":{\"path\":\"fileOld.parquet\",\"partitionValues\":{},\"size\":10,\"modificationTime\":1,\"dataChange\":true}}",
                "{\"add\":{\"path\":\"fileRecent.parquet\",\"partitionValues\":{},\"size\":10,\"modificationTime\":1,\"dataChange\":true}}",
                "{\"add\":{\"path\":\"fileX.parquet\",\"partitionValues\":{},\"size\":10,\"modificationTime\":1,\"dataChange\":true}}"
        ) + "\n");

        Files.writeString(logDir.resolve("00000000000000000001.json"), String.join("\n",
                "{\"remove\":{\"path\":\"fileOld.parquet\",\"deletionTimestamp\":" + OLD + ",\"dataChange\":true}}",
                "{\"remove\":{\"path\":\"fileRecent.parquet\",\"deletionTimestamp\":" + RECENT + ",\"dataChange\":true}}",
                "{\"remove\":{\"path\":\"fileX.parquet\",\"deletionTimestamp\":" + OLD + ",\"dataChange\":true}}"
        ) + "\n");

        // fileX comes back in a later commit -- active again despite the stale remove above.
        Files.writeString(logDir.resolve("00000000000000000002.json"),
                "{\"add\":{\"path\":\"fileX.parquet\",\"partitionValues\":{},\"size\":10,\"modificationTime\":1,\"dataChange\":true}}\n");

        for (String name : List.of("fileOld.parquet", "fileRecent.parquet", "fileX.parquet")) {
            Files.writeString(tableRoot.resolve(name), "dummy parquet bytes");
        }
    }

    @Test
    void vacuum_deletesOldRemovedFiles_keepsRecentRemovedFiles_neverTouchesActiveFiles(@TempDir Path tableRoot) throws Exception {
        writeFixtureHistory(tableRoot);

        List<String> deleted = new Vacuum().vacuum(tableRoot, Duration.ofDays(7));

        assertEquals(List.of("fileOld.parquet"), deleted,
                "only the file removed beyond the retention window gets deleted");

        assertFalse(Files.exists(tableRoot.resolve("fileOld.parquet")), "old removed file must be gone");
        assertTrue(Files.exists(tableRoot.resolve("fileRecent.parquet")), "recently removed file must survive (within retention)");
        assertTrue(Files.exists(tableRoot.resolve("fileX.parquet")),
                "fileX is currently ACTIVE (re-added in v2) -- must survive despite the stale v1 remove entry");

        Snapshot snapshot = new SnapshotManager(tableRoot).latest();
        assertTrue(snapshot.getActiveFiles().stream().anyMatch(f -> f.getPath().equals("fileX.parquet")),
                "fileX is still genuinely part of the live table");
    }

    @Test
    void vacuum_withNoStaleRemovals_deletesNothing(@TempDir Path tableRoot) throws Exception {
        Path logDir = tableRoot.resolve("_delta_log");
        Files.createDirectories(logDir);
        Files.writeString(logDir.resolve("00000000000000000000.json"), String.join("\n",
                "{\"metaData\":{\"id\":\"t\",\"format\":{\"provider\":\"parquet\",\"options\":{}},"
                        + "\"schemaString\":\"{}\",\"partitionColumns\":[],\"configuration\":{},\"createdTime\":1}}",
                "{\"add\":{\"path\":\"only.parquet\",\"partitionValues\":{},\"size\":10,\"modificationTime\":1,\"dataChange\":true}}"
        ) + "\n");
        Files.writeString(tableRoot.resolve("only.parquet"), "dummy");

        List<String> deleted = new Vacuum().vacuum(tableRoot, Duration.ofDays(7));
        assertTrue(deleted.isEmpty());
        assertTrue(Files.exists(tableRoot.resolve("only.parquet")));
    }
}
