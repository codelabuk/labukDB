package com.labuk.labukdb.core.log;

import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import com.labuk.labukdb.core.log.action.AddFile;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ConcurrentModificationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Proves build-order step 1 end to end: {@link CommitWriter#appendBatch}
 * encodes a real Parquet file, commits it atomically, and
 * {@link SnapshotManager} reads the new state back correctly — the exact
 * "append one commit atomically, read it back correctly" bar from
 * {@code docs/labukdb-v2-architecture.md}'s build order.
 */
class CommitWriterTest {

    /** A minimal, real version-0 commit: metaData only, no data files yet — matches FlightIntegrationTest's own self-built-fixture style. */
    private void writeInitialCommit(Path tableRoot, List<String> partitionColumns) throws Exception {
        Path logDir = tableRoot.resolve("_delta_log");
        Files.createDirectories(logDir);

        String schemaString = "{\"type\":\"struct\",\"fields\":["
                + "{\"name\":\"service\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},"
                + "{\"name\":\"amount\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}";

        String partitionColumnsJson = "[" + partitionColumns.stream()
                .map(c -> "\"" + c + "\"").reduce((a, b) -> a + "," + b).orElse("") + "]";

        String commit = String.join("\n",
                "{\"commitInfo\":{\"timestamp\":1732000000000,\"operation\":\"CREATE TABLE\"}}",
                "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}",
                "{\"metaData\":{\"id\":\"commit-writer-test-table\",\"format\":{\"provider\":\"parquet\",\"options\":{}},"
                        + "\"schemaString\":" + "\"" + schemaString.replace("\"", "\\\"") + "\","
                        + "\"partitionColumns\":" + partitionColumnsJson + ",\"configuration\":{},\"createdTime\":1732000000000}}"
        );
        Files.writeString(logDir.resolve("00000000000000000000.json"), commit + "\n");
    }

    private VectorSchemaRoot buildBatch(BufferAllocator allocator, String[] services, long[] amounts) {
        Field serviceField = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field amountField = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(serviceField, amountField)), allocator);
        root.allocateNew();

        VarCharVector serviceVector = (VarCharVector) root.getVector("service");
        BigIntVector amountVector = (BigIntVector) root.getVector("amount");
        for (int i = 0; i < services.length; i++) {
            serviceVector.setSafe(i, services[i].getBytes(StandardCharsets.UTF_8));
            amountVector.setSafe(i, amounts[i]);
        }
        root.setRowCount(services.length);
        return root;
    }

    @Test
    void forLocation_withLocalTableLocation_behavesIdenticallyToTheOriginalConstructor(@TempDir Path tableRoot) throws Exception {
        writeInitialCommit(tableRoot, List.of());

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = buildBatch(allocator, new String[] {"via-forLocation"}, new long[] {7})) {
            long version = CommitWriter.forLocation(com.labuk.labukdb.core.storage.TableLocation.forLocal(tableRoot))
                    .appendBatch(batch, Map.of());
            assertEquals(1, version, "the new TableLocation-based dispatch path must be genuinely wired, not a no-op");
        }

        Snapshot snapshot = new SnapshotManager(tableRoot).latest();
        assertEquals(1, snapshot.getActiveFileCount());
    }

    @Test
    void appendBatch_commitsAtomicallyAndIsReadableBack(@TempDir Path tableRoot) throws Exception {
        writeInitialCommit(tableRoot, List.of("level"));

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            VectorSchemaRoot batch = buildBatch(allocator,
                    new String[] {"auth-svc", "payments-svc", "gateway"},
                    new long[] {1, 2, 3});

            long newVersion;
            try (batch) {
                newVersion = new CommitWriter(tableRoot).appendBatch(batch, Map.of("level", "ERROR"));
            }
            assertEquals(1, newVersion, "first append after version 0 should land as version 1");

            Snapshot snapshot = new SnapshotManager(tableRoot).latest();
            assertEquals(1, snapshot.getVersion());

            Collection<AddFile> activeFiles = snapshot.getActiveFiles();
            assertEquals(1, activeFiles.size(), "exactly one file committed");
            AddFile written = activeFiles.iterator().next();
            assertEquals("ERROR", written.getPartitionValues().get("level"));
            assertTrue(written.getPath().startsWith("level=ERROR/"), "file lands under its partition folder: " + written.getPath());
            assertTrue(written.getPath().endsWith(".snappy.parquet"));
            assertNull(written.getStats(), "stats intentionally left null for this build-order step");

            // The actual "read it back correctly" proof.
            File dataFile = tableRoot.resolve(written.getPath()).toFile();
            assertTrue(dataFile.exists(), "the Parquet file ArrowToParquetWriter wrote must actually exist on disk");

            try (VectorSchemaRoot readBack = new ParquetToArrowReader().read(
                    dataFile, allocator, List.of("service", "amount"), null, Map.of())) {
                assertEquals(3, readBack.getRowCount());
                VarCharVector readService = (VarCharVector) readBack.getVector("service");
                BigIntVector readAmount = (BigIntVector) readBack.getVector("amount");
                assertEquals("auth-svc", new String(readService.get(0), StandardCharsets.UTF_8));
                assertEquals("payments-svc", new String(readService.get(1), StandardCharsets.UTF_8));
                assertEquals("gateway", new String(readService.get(2), StandardCharsets.UTF_8));
                assertEquals(1L, readAmount.get(0));
                assertEquals(2L, readAmount.get(1));
                assertEquals(3L, readAmount.get(2));
            }
        }
    }

    @Test
    void appendBatch_twiceIncrementsVersionEachTime(@TempDir Path tableRoot) throws Exception {
        writeInitialCommit(tableRoot, List.of());

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            CommitWriter writer = new CommitWriter(tableRoot);

            try (VectorSchemaRoot batch1 = buildBatch(allocator, new String[] {"a"}, new long[] {1})) {
                assertEquals(1, writer.appendBatch(batch1, Map.of()));
            }
            try (VectorSchemaRoot batch2 = buildBatch(allocator, new String[] {"b"}, new long[] {2})) {
                assertEquals(2, writer.appendBatch(batch2, Map.of()));
            }

            Snapshot snapshot = new SnapshotManager(tableRoot).latest();
            assertEquals(2, snapshot.getVersion());
            assertEquals(2, snapshot.getActiveFiles().size());
        }
    }

    @Test
    void appendBatch_missingPartitionValue_throws(@TempDir Path tableRoot) throws Exception {
        writeInitialCommit(tableRoot, List.of("level"));

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = buildBatch(allocator, new String[] {"a"}, new long[] {1})) {
            assertThrows(IllegalArgumentException.class,
                    () -> new CommitWriter(tableRoot).appendBatch(batch, Map.of()));
        }
    }

    /**
     * Real concurrency, not a simulated single conflict: N independent
     * {@link CommitWriter}s (simulating N separate processes, not one
     * shared instance) race {@code appendBatch} against the same table at
     * once. With plain {@code CREATE_NEW} semantics and no retry logic,
     * only one of these would ever land and the rest would throw — this
     * is the actual proof the optimistic-concurrency retry loop survives
     * real contention, not just a hand-simulated one-shot conflict.
     */
    @Test
    void appendBatch_concurrentWriters_allCommitsLandWithNoLostUpdates(@TempDir Path tableRoot) throws Exception {
        writeInitialCommit(tableRoot, List.of());
        int writerCount = 10;

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            ExecutorService pool = Executors.newFixedThreadPool(writerCount);
            CountDownLatch startGate = new CountDownLatch(1);
            List<Future<Long>> futures = new ArrayList<>();

            for (int i = 0; i < writerCount; i++) {
                int id = i;
                futures.add(pool.submit(() -> {
                    startGate.await();
                    CommitWriter writer = new CommitWriter(tableRoot);
                    try (VectorSchemaRoot batch = buildBatch(allocator, new String[] {"writer-" + id}, new long[] {id})) {
                        return writer.appendBatch(batch, Map.of());
                    }
                }));
            }

            startGate.countDown(); // release all writers at once -- maximizes real collisions
            Set<Long> versions = new HashSet<>();
            for (Future<Long> future : futures) {
                versions.add(future.get(30, TimeUnit.SECONDS));
            }
            pool.shutdown();

            assertEquals(writerCount, versions.size(),
                    "every writer landed under a distinct version -- none lost, none double-assigned");

            Snapshot finalSnapshot = new SnapshotManager(tableRoot).latest();
            assertEquals(writerCount, finalSnapshot.getVersion());
            assertEquals(writerCount, finalSnapshot.getActiveFiles().size());

            Set<String> expectedServices = new HashSet<>();
            for (int i = 0; i < writerCount; i++) {
                expectedServices.add("writer-" + i);
            }
            Set<String> actualServices = new HashSet<>();
            for (AddFile file : finalSnapshot.getActiveFiles()) {
                File dataFile = tableRoot.resolve(file.getPath()).toFile();
                try (VectorSchemaRoot readBack = new ParquetToArrowReader().read(
                        dataFile, allocator, List.of("service"), null, Map.of())) {
                    actualServices.add(new String(
                            ((VarCharVector) readBack.getVector("service")).get(0), StandardCharsets.UTF_8));
                }
            }
            assertEquals(expectedServices, actualServices, "every writer's row is present and correct -- no lost updates");
        }
    }

    @Test
    void replaceFiles_fileAlreadyRemovedByConcurrentCommit_throwsInsteadOfRetryingBlindly(@TempDir Path tableRoot) throws Exception {
        writeInitialCommit(tableRoot, List.of());

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            CommitWriter writer = new CommitWriter(tableRoot);
            try (VectorSchemaRoot b1 = buildBatch(allocator, new String[] {"a"}, new long[] {1})) {
                writer.appendBatch(b1, Map.of());
            }
            try (VectorSchemaRoot b2 = buildBatch(allocator, new String[] {"b"}, new long[] {2})) {
                writer.appendBatch(b2, Map.of());
            }

            Snapshot beforeCompact = new SnapshotManager(tableRoot).latest();
            List<AddFile> filesToCompact = new ArrayList<>(beforeCompact.getActiveFiles());
            assertEquals(2, filesToCompact.size());
            AddFile alreadyRemovedElsewhere = filesToCompact.get(0);

            // Simulate a concurrent writer (e.g. a racing Compactor) that
            // already removed one of these files, committed directly as the
            // next version -- same raw-JSON-commit convention writeInitialCommit
            // already uses elsewhere in this file.
            long nextVersion = beforeCompact.getVersion() + 1;
            String competingRemove = "{\"remove\":{\"path\":\""
                    + alreadyRemovedElsewhere.getPath().replace("\\", "\\\\")
                    + "\",\"deletionTimestamp\":" + System.currentTimeMillis() + ",\"dataChange\":false}}\n";
            Files.writeString(tableRoot.resolve("_delta_log").resolve(CommitFileNames.jsonFileName(nextVersion)), competingRemove);

            try (VectorSchemaRoot merged = buildBatch(allocator, new String[] {"merged"}, new long[] {99})) {
                assertThrows(ConcurrentModificationException.class,
                        () -> writer.replaceFiles(filesToCompact, merged, Map.of(), false),
                        "one of the files to compact was already removed by a concurrent commit -- must not retry blindly");
            }
        }
    }
}
