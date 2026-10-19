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
import java.util.Collection;
import java.util.List;
import java.util.Map;

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
}
