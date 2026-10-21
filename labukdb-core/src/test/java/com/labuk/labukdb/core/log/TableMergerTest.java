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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Proves the upsert/MERGE-by-key foundation {@code TableMerger} adds on
 * top of {@link CommitWriter}: insert/update/delete by key, correctly
 * merged with whatever's already in the target partition, committed
 * atomically with {@code dataChange=true}. Schema throughout: {@code id}
 * (long, the key) + {@code service} (string, the mutable attribute) —
 * same two-column shape {@code CommitWriterTest} already uses, just with
 * {@code id} standing in for {@code amount}.
 */
class TableMergerTest {

    private void writeInitialCommit(Path tableRoot, List<String> partitionColumns) throws Exception {
        Path logDir = tableRoot.resolve("_delta_log");
        Files.createDirectories(logDir);

        String schemaString = "{\"type\":\"struct\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},"
                + "{\"name\":\"service\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}";

        String partitionColumnsJson = "[" + partitionColumns.stream()
                .map(c -> "\"" + c + "\"").reduce((a, b) -> a + "," + b).orElse("") + "]";

        String commit = String.join("\n",
                "{\"commitInfo\":{\"timestamp\":1732000000000,\"operation\":\"CREATE TABLE\"}}",
                "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}",
                "{\"metaData\":{\"id\":\"table-merger-test-table\",\"format\":{\"provider\":\"parquet\",\"options\":{}},"
                        + "\"schemaString\":" + "\"" + schemaString.replace("\"", "\\\"") + "\","
                        + "\"partitionColumns\":" + partitionColumnsJson + ",\"configuration\":{},\"createdTime\":1732000000000}}"
        );
        Files.writeString(logDir.resolve("00000000000000000000.json"), commit + "\n");
    }

    private VectorSchemaRoot buildBatch(BufferAllocator allocator, long[] ids, String[] services) {
        Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field serviceField = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(idField, serviceField)), allocator);
        root.allocateNew();
        BigIntVector idVector = (BigIntVector) root.getVector("id");
        VarCharVector serviceVector = (VarCharVector) root.getVector("service");
        for (int i = 0; i < ids.length; i++) {
            idVector.setSafe(i, ids[i]);
            serviceVector.setSafe(i, services[i].getBytes(StandardCharsets.UTF_8));
        }
        root.setRowCount(ids.length);
        return root;
    }

    /** id -> service, read back from every active file in the given partition. */
    private Map<Long, String> readBackByPartition(Path tableRoot, Map<String, String> partitionValues,
                                                    BufferAllocator allocator) throws Exception {
        Snapshot snapshot = new SnapshotManager(tableRoot).latest();
        Map<Long, String> result = new HashMap<>();
        for (AddFile file : snapshot.getActiveFiles()) {
            if (!file.getPartitionValues().equals(partitionValues)) {
                continue;
            }
            File dataFile = tableRoot.resolve(file.getPath()).toFile();
            try (VectorSchemaRoot readBack = new ParquetToArrowReader().read(
                    dataFile, allocator, List.of("id", "service"), null, Map.of())) {
                BigIntVector idVector = (BigIntVector) readBack.getVector("id");
                VarCharVector serviceVector = (VarCharVector) readBack.getVector("service");
                for (int i = 0; i < readBack.getRowCount(); i++) {
                    result.put(idVector.get(i), new String(serviceVector.get(i), StandardCharsets.UTF_8));
                }
            }
        }
        return result;
    }

    @Test
    void merge_insertOnly_intoBrandNewPartition_createsOneFile(@TempDir Path tableRoot) throws Exception {
        writeInitialCommit(tableRoot, List.of());
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            try (VectorSchemaRoot inserts = buildBatch(allocator, new long[] {1, 2}, new String[] {"auth-svc", "payments-svc"})) {
                long version = new TableMerger().merge(tableRoot, Map.of(), List.of("id"),
                        inserts, null, null, allocator);
                assertEquals(1, version);
            }
            Map<Long, String> rows = readBackByPartition(tableRoot, Map.of(), allocator);
            assertEquals(Map.of(1L, "auth-svc", 2L, "payments-svc"), rows);
        }
    }

    @Test
    void merge_updateByKey_replacesOldValue(@TempDir Path tableRoot) throws Exception {
        writeInitialCommit(tableRoot, List.of());
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            try (VectorSchemaRoot existing = buildBatch(allocator, new long[] {1}, new String[] {"old-name"})) {
                new CommitWriter(tableRoot).appendBatch(existing, Map.of());
            }
            try (VectorSchemaRoot updates = buildBatch(allocator, new long[] {1}, new String[] {"new-name"})) {
                new TableMerger().merge(tableRoot, Map.of(), List.of("id"), null, updates, null, allocator);
            }
            Map<Long, String> rows = readBackByPartition(tableRoot, Map.of(), allocator);
            assertEquals(Map.of(1L, "new-name"), rows, "the old value must be gone, not just appended alongside");
        }
    }

    @Test
    void merge_deleteByKey_removesOnlyThatRow(@TempDir Path tableRoot) throws Exception {
        writeInitialCommit(tableRoot, List.of());
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            try (VectorSchemaRoot existing = buildBatch(allocator, new long[] {1, 2}, new String[] {"keep-me", "delete-me"})) {
                new CommitWriter(tableRoot).appendBatch(existing, Map.of());
            }
            try (VectorSchemaRoot deletes = buildBatch(allocator, new long[] {2}, new String[] {"ignored"})) {
                new TableMerger().merge(tableRoot, Map.of(), List.of("id"), null, null, deletes, allocator);
            }
            Map<Long, String> rows = readBackByPartition(tableRoot, Map.of(), allocator);
            assertEquals(Map.of(1L, "keep-me"), rows);
        }
    }

    @Test
    void merge_deleteEveryRowInPartition_commitsRemovesOnlyAndLeavesNoActiveFiles(@TempDir Path tableRoot) throws Exception {
        writeInitialCommit(tableRoot, List.of());
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            try (VectorSchemaRoot existing = buildBatch(allocator, new long[] {1}, new String[] {"only-row"})) {
                new CommitWriter(tableRoot).appendBatch(existing, Map.of());
            }
            try (VectorSchemaRoot deletes = buildBatch(allocator, new long[] {1}, new String[] {"ignored"})) {
                new TableMerger().merge(tableRoot, Map.of(), List.of("id"), null, null, deletes, allocator);
            }
            Snapshot after = new SnapshotManager(tableRoot).latest();
            long remainingInPartition = after.getActiveFiles().stream()
                    .filter(f -> f.getPartitionValues().equals(Map.of())).count();
            assertEquals(0, remainingInPartition, "deleting every row must leave zero active files, via a removes-only commit");
        }
    }

    @Test
    void merge_mixedInsertUpdateDelete_acrossMultiplePreExistingFiles(@TempDir Path tableRoot) throws Exception {
        writeInitialCommit(tableRoot, List.of());
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            CommitWriter writer = new CommitWriter(tableRoot);
            // Three separate commits -> three separate files in the same partition,
            // proving merge reads/combines across files, not just one.
            try (VectorSchemaRoot b1 = buildBatch(allocator, new long[] {1}, new String[] {"keep"})) {
                writer.appendBatch(b1, Map.of());
            }
            try (VectorSchemaRoot b2 = buildBatch(allocator, new long[] {2}, new String[] {"update-me"})) {
                writer.appendBatch(b2, Map.of());
            }
            try (VectorSchemaRoot b3 = buildBatch(allocator, new long[] {3}, new String[] {"delete-me"})) {
                writer.appendBatch(b3, Map.of());
            }

            try (VectorSchemaRoot inserts = buildBatch(allocator, new long[] {4}, new String[] {"brand-new"});
                 VectorSchemaRoot updates = buildBatch(allocator, new long[] {2}, new String[] {"updated"});
                 VectorSchemaRoot deletes = buildBatch(allocator, new long[] {3}, new String[] {"ignored"})) {
                new TableMerger().merge(tableRoot, Map.of(), List.of("id"), inserts, updates, deletes, allocator);
            }

            Map<Long, String> rows = readBackByPartition(tableRoot, Map.of(), allocator);
            assertEquals(Map.of(1L, "keep", 2L, "updated", 4L, "brand-new"), rows);

            Snapshot after = new SnapshotManager(tableRoot).latest();
            long filesInPartition = after.getActiveFiles().stream().filter(f -> f.getPartitionValues().equals(Map.of())).count();
            assertEquals(1, filesInPartition, "the three original files plus the merge result must collapse into exactly one new file");
        }
    }

    @Test
    void merge_missingKeyColumn_throwsClearValidationError(@TempDir Path tableRoot) throws Exception {
        writeInitialCommit(tableRoot, List.of());
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot inserts = buildBatch(allocator, new long[] {1}, new String[] {"x"})) {
            IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                    () -> new TableMerger().merge(tableRoot, Map.of(), List.of("does_not_exist"),
                            inserts, null, null, allocator));
            assertTrue(thrown.getMessage().contains("does_not_exist"), "error must name the missing column: " + thrown.getMessage());
        }
    }

    @Test
    void merge_concurrentAppendToSamePartition_bothWritersLandNoDataLost(@TempDir Path tableRoot) throws Exception {
        writeInitialCommit(tableRoot, List.of());
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            try (VectorSchemaRoot existing = buildBatch(allocator, new long[] {1}, new String[] {"original"})) {
                new CommitWriter(tableRoot).appendBatch(existing, Map.of());
            }

            ExecutorService pool = Executors.newFixedThreadPool(2);
            CountDownLatch gate = new CountDownLatch(1);

            Future<?> mergeFuture = pool.submit(() -> {
                try {
                    gate.await();
                    try (VectorSchemaRoot inserts = buildBatch(allocator, new long[] {2}, new String[] {"from-merge"})) {
                        new TableMerger().merge(tableRoot, Map.of(), List.of("id"), inserts, null, null, allocator);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            Future<?> appendFuture = pool.submit(() -> {
                try {
                    gate.await();
                    try (VectorSchemaRoot concurrent = buildBatch(allocator, new long[] {3}, new String[] {"from-concurrent-append"})) {
                        new CommitWriter(tableRoot).appendBatch(concurrent, Map.of());
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            gate.countDown();
            mergeFuture.get(30, TimeUnit.SECONDS);
            appendFuture.get(30, TimeUnit.SECONDS);
            pool.shutdown();

            Map<Long, String> rows = readBackByPartition(tableRoot, Map.of(), allocator);
            assertEquals(Map.of(1L, "original", 2L, "from-merge", 3L, "from-concurrent-append"), rows,
                    "a concurrent plain append racing a merge in the same partition must not lose either writer's row, "
                            + "and the merge's own freshness check must not silently drop the concurrently-added file");
        }
    }
}
