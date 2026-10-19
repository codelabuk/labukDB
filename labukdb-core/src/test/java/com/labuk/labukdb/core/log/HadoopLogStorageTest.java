package com.labuk.labukdb.core.log;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Proves {@code createCommitFile}'s atomicity contract on the one backend
 * actually testable in this environment — Hadoop's local filesystem
 * implementation, the same substitution
 * {@link HadoopBackedSnapshotManagerTest} already uses for the read path.
 *
 * This does NOT prove S3/MinIO/ADLS behave the same way — see
 * {@link HadoopLogStorage}'s javadoc for the differentiated, per-backend
 * confidence level and why only this local substitution is verifiable
 * from here.
 */
class HadoopLogStorageTest {

    @Test
    void createCommitFile_writesContentReadableBack(@TempDir Path tempDir) throws Exception {
        Path logDir = tempDir.resolve("_delta_log");
        java.nio.file.Files.createDirectories(logDir);
        HadoopLogStorage storage = new HadoopLogStorage(
                new org.apache.hadoop.fs.Path(logDir.toString()), new Configuration());

        storage.createCommitFile("00000000000000000000.json", "{\"commitInfo\":{}}\n");

        try (BufferedReader reader = storage.openReader("00000000000000000000.json")) {
            assertEquals("{\"commitInfo\":{}}", reader.readLine());
            assertNull(reader.readLine());
        }
        assertTrue(storage.listCommitFileNames().contains("00000000000000000000.json"));
    }

    @Test
    void createCommitFile_sameNameTwice_secondCallThrows(@TempDir Path tempDir) throws Exception {
        Path logDir = tempDir.resolve("_delta_log");
        java.nio.file.Files.createDirectories(logDir);
        HadoopLogStorage storage = new HadoopLogStorage(
                new org.apache.hadoop.fs.Path(logDir.toString()), new Configuration());

        storage.createCommitFile("00000000000000000000.json", "{\"commitInfo\":{}}\n");

        assertThrows(FileAlreadyExistsException.class,
                () -> storage.createCommitFile("00000000000000000000.json", "{\"a different payload\":{}}\n"));

        // The first writer's content must survive untouched — proves this
        // isn't a silent overwrite, the whole point of the contract.
        try (BufferedReader reader = storage.openReader("00000000000000000000.json")) {
            assertEquals("{\"commitInfo\":{}}", reader.readLine());
        }
    }

    @Test
    void createCommitFile_matchesLocalNioLogStorage_forTheSameDirectory(@TempDir Path tempDir) throws Exception {
        Path logDir = tempDir.resolve("_delta_log");
        java.nio.file.Files.createDirectories(logDir);

        LocalNioLogStorage local = new LocalNioLogStorage(logDir);
        local.createCommitFile("00000000000000000000.json", "{\"commitInfo\":{}}\n");

        HadoopLogStorage hadoopBacked = new HadoopLogStorage(
                new org.apache.hadoop.fs.Path(logDir.toString()), new Configuration());

        // Both implementations point at the same directory — the Hadoop-
        // backed one must see exactly what the local one wrote, proving
        // this isn't a parallel, divergent storage mechanism.
        assertEquals(local.listCommitFileNames(), hadoopBacked.listCommitFileNames());
        try (BufferedReader reader = hadoopBacked.openReader("00000000000000000000.json")) {
            assertEquals("{\"commitInfo\":{}}", reader.readLine());
        }
    }
}
