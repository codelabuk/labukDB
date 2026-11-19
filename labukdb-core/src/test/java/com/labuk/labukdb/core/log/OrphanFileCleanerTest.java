package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.storage.TableLocation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link OrphanFileCleaner} covers the gap {@link Vacuum} genuinely
 * doesn't: a Parquet file sitting in table storage that no commit, past
 * or present, ever mentioned at all -- not even as a {@code remove}
 * tombstone. These tests build that scenario by hand (a file written to
 * disk with no matching commit action), the same hand-written-JSON-fixture
 * style {@link VacuumTest} already uses, so the two classes' tests stay
 * directly comparable.
 */
class OrphanFileCleanerTest {

    private static final long NOW = System.currentTimeMillis();

    private void writeFixtureHistory(Path tableRoot) throws Exception {
        Path logDir = tableRoot.resolve("_delta_log");
        Files.createDirectories(logDir);

        String metaData = "{\"metaData\":{\"id\":\"orphan-test\",\"format\":{\"provider\":\"parquet\",\"options\":{}},"
                + "\"schemaString\":\"{}\",\"partitionColumns\":[],\"configuration\":{},\"createdTime\":1732000000000}}";

        Files.writeString(logDir.resolve("00000000000000000000.json"), String.join("\n",
                "{\"commitInfo\":{\"timestamp\":1732000000000,\"operation\":\"CREATE TABLE\"}}",
                "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}",
                metaData,
                "{\"add\":{\"path\":\"active.parquet\",\"partitionValues\":{},\"size\":10,\"modificationTime\":1,\"dataChange\":true}}"
        ) + "\n");

        // A second commit: addedThenRemoved.parquet was added and then
        // removed in the same version -- Vacuum's own job once it's past
        // ITS retention, not an orphan here regardless of age.
        Files.writeString(logDir.resolve("00000000000000000001.json"), String.join("\n",
                "{\"add\":{\"path\":\"addedThenRemoved.parquet\",\"partitionValues\":{},\"size\":10,\"modificationTime\":1,\"dataChange\":true}}",
                "{\"remove\":{\"path\":\"addedThenRemoved.parquet\",\"deletionTimestamp\":" + NOW + ",\"dataChange\":true}}"
        ) + "\n");

        writeDataFile(tableRoot, "active.parquet", NOW);
        writeDataFile(tableRoot, "addedThenRemoved.parquet", NOW - Duration.ofDays(30).toMillis());
    }

    private void writeDataFile(Path tableRoot, String relativePath, long modificationTimeMillis) throws Exception {
        Path file = tableRoot.resolve(relativePath);
        Files.createDirectories(file.getParent());
        Files.writeString(file, "dummy parquet bytes");
        Files.setLastModifiedTime(file, FileTime.fromMillis(modificationTimeMillis));
    }

    @Test
    void trueOrphan_olderThanRetention_isDeleted(@TempDir Path tableRoot) throws Exception {
        writeFixtureHistory(tableRoot);
        writeDataFile(tableRoot, "level=ERROR/orphan.parquet", NOW - Duration.ofDays(10).toMillis());

        List<String> deleted = new OrphanFileCleaner().clean(TableLocation.forLocal(tableRoot), Duration.ofDays(7));

        assertEquals(List.of("level=ERROR/orphan.parquet"), deleted,
                "only the genuinely unmentioned, old file is an orphan");
        assertFalse(Files.exists(tableRoot.resolve("level=ERROR/orphan.parquet")));
    }

    @Test
    void trueOrphan_withinRetentionWindow_isKept_mightBeAnInFlightWriter(@TempDir Path tableRoot) throws Exception {
        writeFixtureHistory(tableRoot);
        writeDataFile(tableRoot, "freshOrphan.parquet", NOW - Duration.ofHours(1).toMillis());

        List<String> deleted = new OrphanFileCleaner().clean(TableLocation.forLocal(tableRoot), Duration.ofDays(7));

        assertTrue(deleted.isEmpty(), "a recent, unmentioned file could be an in-flight writer -- never deleted within retention");
        assertTrue(Files.exists(tableRoot.resolve("freshOrphan.parquet")));
    }

    @Test
    void activeFile_neverTouchedRegardlessOfAge(@TempDir Path tableRoot) throws Exception {
        writeFixtureHistory(tableRoot);
        // active.parquet's own modification time (NOW) is well within retention anyway,
        // but the real point: it's mentioned in the log, so age wouldn't matter either way.

        new OrphanFileCleaner().clean(TableLocation.forLocal(tableRoot), Duration.ofDays(7));

        assertTrue(Files.exists(tableRoot.resolve("active.parquet")), "a file the log actually knows about is never an orphan");
    }

    @Test
    void addedThenRemovedFile_oldEnoughForOrphanRetention_stillNotTouched_thatsVacuumsJob(@TempDir Path tableRoot) throws Exception {
        writeFixtureHistory(tableRoot);
        // addedThenRemoved.parquet is 30 days old -- well past OrphanFileCleaner's
        // own retention -- but it WAS mentioned (add, then remove), so it must
        // survive here regardless: deleting it is Vacuum's responsibility, not this class's.

        List<String> deleted = new OrphanFileCleaner().clean(TableLocation.forLocal(tableRoot), Duration.ofDays(7));

        assertFalse(deleted.contains("addedThenRemoved.parquet"));
        assertTrue(Files.exists(tableRoot.resolve("addedThenRemoved.parquet")),
                "a file the log mentions, even only as an old remove tombstone, is not this class's job");
    }

    @Test
    void noOrphansAtAll_deletesNothing(@TempDir Path tableRoot) throws Exception {
        writeFixtureHistory(tableRoot);

        List<String> deleted = new OrphanFileCleaner().clean(TableLocation.forLocal(tableRoot), Duration.ofDays(7));

        assertTrue(deleted.isEmpty());
    }

    @Test
    void nonParquetFileInTableDirectory_neverConsideredADeletionCandidate(@TempDir Path tableRoot) throws Exception {
        writeFixtureHistory(tableRoot);
        writeDataFile(tableRoot, "README.txt", NOW - Duration.ofDays(30).toMillis());

        List<String> deleted = new OrphanFileCleaner().clean(TableLocation.forLocal(tableRoot), Duration.ofDays(7));

        assertFalse(deleted.contains("README.txt"));
        assertTrue(Files.exists(tableRoot.resolve("README.txt")));
    }
}
