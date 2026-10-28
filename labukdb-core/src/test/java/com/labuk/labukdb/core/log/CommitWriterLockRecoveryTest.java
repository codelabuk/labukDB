package com.labuk.labukdb.core.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.labuk.labukdb.core.log.action.AddFile;
import com.labuk.labukdb.core.log.action.SingleAction;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deterministic, local-disk, always-runnable proof of {@link
 * CommitWriter}'s crash-recovery branch ({@code recoverEntry}/{@code
 * catchUpViaLock}) — the one thing the live-infra tests
 * ({@code CommitWriterConcurrentMinioLockedIntegrationTest}) can't exercise,
 * since 8 simultaneous non-crashing writers never leave a stuck incomplete
 * entry behind. Uses a trivial in-memory {@link LockProvider} test double
 * instead of a real DynamoDB backend — the recovery logic under test lives
 * entirely in {@link CommitWriter}, not in any particular {@link
 * LockProvider} implementation.
 */
class CommitWriterLockRecoveryTest {

    /** Minimal in-memory LockProvider, no concurrency control of its own -- single-threaded test only. */
    static final class InMemoryLockProvider implements LockProvider {
        final Map<String, ExternalCommitEntry> entries = new HashMap<>();

        private static String key(String tablePath, String fileName) {
            return tablePath + "::" + fileName;
        }

        @Override
        public void putEntry(ExternalCommitEntry entry, boolean overwrite) throws java.io.IOException {
            String k = key(entry.tablePath(), entry.fileName());
            if (!overwrite && entries.containsKey(k)) {
                throw new java.nio.file.FileAlreadyExistsException(k);
            }
            entries.put(k, entry);
        }

        @Override
        public Optional<ExternalCommitEntry> getEntry(String tablePath, String fileName) {
            return Optional.ofNullable(entries.get(key(tablePath, fileName)));
        }
    }

    private void writeInitialCommit(Path tableRoot) throws Exception {
        Path logDir = tableRoot.resolve("_delta_log");
        Files.createDirectories(logDir);
        String schemaString = "{\"type\":\"struct\",\"fields\":["
                + "{\"name\":\"service\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},"
                + "{\"name\":\"amount\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}";
        String commit = String.join("\n",
                "{\"commitInfo\":{\"timestamp\":1732000000000,\"operation\":\"CREATE TABLE\"}}",
                "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}",
                "{\"metaData\":{\"id\":\"lock-recovery-test-table\",\"format\":{\"provider\":\"parquet\",\"options\":{}},"
                        + "\"schemaString\":\"" + schemaString.replace("\"", "\\\"") + "\","
                        + "\"partitionColumns\":[],\"configuration\":{},\"createdTime\":1732000000000}}"
        );
        Files.writeString(logDir.resolve("00000000000000000000.json"), commit + "\n");
    }

    private VectorSchemaRoot buildBatch(BufferAllocator allocator, String service, long amount) {
        Field serviceField = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field amountField = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(serviceField, amountField)), allocator);
        root.allocateNew();
        ((VarCharVector) root.getVector("service")).setSafe(0, service.getBytes(StandardCharsets.UTF_8));
        ((BigIntVector) root.getVector("amount")).setSafe(0, amount);
        root.setRowCount(1);
        return root;
    }

    /**
     * Pre-seeds the exact situation a crashed writer would leave behind:
     * version 1's real data file and temp commit-JSON both exist on disk
     * (written by the "crashed" writer before it died), but the final
     * {@code 00000000000000000001.json} does not, and the lock entry for
     * it is still {@code complete=false}.
     */
    private void seedStuckVersionOne(Path tableRoot, InMemoryLockProvider lock, String tablePath) throws Exception {
        String dataFileName = "part-stuck-writer.snappy.parquet";
        File dataFile = tableRoot.resolve(dataFileName).toFile();
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = buildBatch(allocator, "crashed-writer-row", 999)) {
            new com.labuk.labukdb.core.encode.ArrowToParquetWriter().write(dataFile, batch);
        }

        AddFile addFile = new AddFile(dataFileName, Map.of(), dataFile.length(),
                System.currentTimeMillis(), true, null);
        String content = new ObjectMapper().writeValueAsString(SingleAction.ofAdd(addFile)) + "\n";

        String tempPath = ".tmp/stuck-writer-temp.json";
        Files.createDirectories(tableRoot.resolve("_delta_log").resolve(".tmp"));
        Files.writeString(tableRoot.resolve("_delta_log").resolve(tempPath), content);

        lock.entries.put(tablePath + "::" + CommitFileNames.jsonFileName(1),
                new ExternalCommitEntry(tablePath, CommitFileNames.jsonFileName(1), tempPath, false, null));
    }

    @Test
    void appendBatch_withStuckPriorEntry_recoversItThenCommitsOnTop(@TempDir Path tableRoot) throws Exception {
        writeInitialCommit(tableRoot);
        TableLocation location = TableLocation.forLocal(tableRoot);
        InMemoryLockProvider lock = new InMemoryLockProvider();
        seedStuckVersionOne(tableRoot, lock, location.raw());

        // Sanity check: before recovery, version 1's real file genuinely does not exist yet.
        assertFalse(Files.exists(tableRoot.resolve("_delta_log").resolve("00000000000000000001.json")),
                "test setup invariant: the stuck version's final file must not exist before recovery");

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot newBatch = buildBatch(allocator, "new-writer-row", 42)) {
            long version = CommitWriter.forLocation(location, lock).appendBatch(newBatch, Map.of());
            assertEquals(2, version, "the new commit must land as version 2, after the recovered version 1");
        }

        assertTrue(Files.exists(tableRoot.resolve("_delta_log").resolve("00000000000000000001.json")),
                "recovery must have written the previously-missing final file for version 1");
        assertTrue(lock.getEntry(location.raw(), CommitFileNames.jsonFileName(1)).orElseThrow().complete(),
                "the recovered entry must be marked complete");

        Snapshot finalSnapshot = new SnapshotManager(tableRoot).latest();
        assertEquals(2, finalSnapshot.getVersion());
        assertEquals(2, finalSnapshot.getActiveFiles().size(), "both the recovered file and the new commit's file must be active");

        java.util.Set<String> paths = finalSnapshot.getActiveFiles().stream()
                .map(AddFile::getPath).collect(java.util.stream.Collectors.toSet());
        assertTrue(paths.contains("part-stuck-writer.snappy.parquet"), "the recovered file must be visible");
    }
}
