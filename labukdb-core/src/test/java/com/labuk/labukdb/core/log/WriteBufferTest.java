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
 * Proves build-order step 2 end to end: {@link WriteBuffer} buffers
 * durably, auto-flushes on threshold, survives a simulated crash (a
 * second instance picking up the first's un-flushed WAL files), and
 * remains correct and reusable across multiple flush cycles.
 */
class WriteBufferTest {

    private void writeInitialCommit(Path tableRoot) throws Exception {
        Path logDir = tableRoot.resolve("_delta_log");
        Files.createDirectories(logDir);

        String schemaString = "{\"type\":\"struct\",\"fields\":["
                + "{\"name\":\"service\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},"
                + "{\"name\":\"amount\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}";

        String commit = String.join("\n",
                "{\"commitInfo\":{\"timestamp\":1732000000000,\"operation\":\"CREATE TABLE\"}}",
                "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}",
                "{\"metaData\":{\"id\":\"write-buffer-test-table\",\"format\":{\"provider\":\"parquet\",\"options\":{}},"
                        + "\"schemaString\":\"" + schemaString.replace("\"", "\\\"") + "\","
                        + "\"partitionColumns\":[\"level\"],\"configuration\":{},\"createdTime\":1732000000000}}"
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
    void append_belowThreshold_doesNotFlush(@TempDir Path tableRoot) throws Exception {
        writeInitialCommit(tableRoot);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             WriteBuffer writeBuffer = new WriteBuffer(tableRoot, Map.of("level", "ERROR"), 100, 10_000_000, allocator)) {
            try (VectorSchemaRoot batch = buildBatch(allocator, new String[] {"a"}, new long[] {1})) {
                writeBuffer.append(batch);
            }

            assertEquals(1, writeBuffer.bufferedRowCount());
            assertEquals(0, new SnapshotManager(tableRoot).latest().getVersion(),
                    "nothing should be committed yet — still under threshold");
        }
    }

    @Test
    void append_crossingRowThreshold_autoFlushesWithCorrectConcatenatedRows(@TempDir Path tableRoot) throws Exception {
        writeInitialCommit(tableRoot);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             WriteBuffer writeBuffer = new WriteBuffer(tableRoot, Map.of("level", "ERROR"), 3, 10_000_000, allocator)) {
            try (VectorSchemaRoot b1 = buildBatch(allocator, new String[] {"a", "b"}, new long[] {1, 2})) {
                writeBuffer.append(b1);
            }
            assertEquals(0, new SnapshotManager(tableRoot).latest().getVersion(), "2 rows buffered, threshold is 3 — no flush yet");

            try (VectorSchemaRoot b2 = buildBatch(allocator, new String[] {"c"}, new long[] {3})) {
                writeBuffer.append(b2); // crosses the 3-row threshold -> auto-flush
            }

            assertEquals(0, writeBuffer.bufferedRowCount(), "buffer reset after auto-flush");
            Snapshot snapshot = new SnapshotManager(tableRoot).latest();
            assertEquals(1, snapshot.getVersion());

            Collection<AddFile> files = snapshot.getActiveFiles();
            assertEquals(1, files.size());
            AddFile written = files.iterator().next();

            File dataFile = tableRoot.resolve(written.getPath()).toFile();
            try (VectorSchemaRoot readBack = new ParquetToArrowReader().read(
                    dataFile, allocator, List.of("service", "amount"), null, Map.of())) {
                assertEquals(3, readBack.getRowCount(), "both buffered batches concatenated into one file");
                VarCharVector readService = (VarCharVector) readBack.getVector("service");
                BigIntVector readAmount = (BigIntVector) readBack.getVector("amount");
                assertEquals("a", new String(readService.get(0), StandardCharsets.UTF_8));
                assertEquals("b", new String(readService.get(1), StandardCharsets.UTF_8));
                assertEquals("c", new String(readService.get(2), StandardCharsets.UTF_8));
                assertEquals(1L, readAmount.get(0));
                assertEquals(2L, readAmount.get(1));
                assertEquals(3L, readAmount.get(2));
            }
        }
    }

    @Test
    void secondInstance_recoversUnflushedWalFromFirst_simulatingACrash(@TempDir Path tableRoot) throws Exception {
        writeInitialCommit(tableRoot);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            // First "process": appends, stays under threshold, never flushes or closes cleanly.
            try (WriteBuffer first = new WriteBuffer(tableRoot, Map.of("level", "ERROR"), 100, 10_000_000, allocator);
                 VectorSchemaRoot batch = buildBatch(allocator, new String[] {"x", "y"}, new long[] {10, 20})) {
                first.append(batch);
                assertEquals(2, first.bufferedRowCount());
            }
            // No flush happened — "first" just goes out of scope here, like a crashed process.

            // Second "process": a fresh WriteBuffer against the same table + partition.
            try (WriteBuffer second = new WriteBuffer(tableRoot, Map.of("level", "ERROR"), 100, 10_000_000, allocator)) {
                assertEquals(2, second.bufferedRowCount(), "recovered the first instance's un-flushed WAL on construction");

                long version = second.flush();
                assertEquals(1, version);

                Snapshot snapshot = new SnapshotManager(tableRoot).latest();
                assertEquals(1, snapshot.getActiveFileCount());
            }
        }
    }

    @Test
    void flush_belowThreshold_stillCommitsAndClearsBuffer(@TempDir Path tableRoot) throws Exception {
        writeInitialCommit(tableRoot);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             WriteBuffer writeBuffer = new WriteBuffer(tableRoot, Map.of("level", "ERROR"), 1000, 10_000_000, allocator);
             VectorSchemaRoot batch = buildBatch(allocator, new String[] {"a"}, new long[] {1})) {
            writeBuffer.append(batch);
            assertEquals(1, writeBuffer.bufferedRowCount());

            long version = writeBuffer.flush();
            assertEquals(1, version);
            assertEquals(0, writeBuffer.bufferedRowCount());
            assertEquals(-1, writeBuffer.flush(), "flushing an empty buffer is a no-op");
        }
    }

    @Test
    void multipleAppendFlushCycles_eachIndependentlyCorrect(@TempDir Path tableRoot) throws Exception {
        writeInitialCommit(tableRoot);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             WriteBuffer writeBuffer = new WriteBuffer(tableRoot, Map.of("level", "ERROR"), 1, 10_000_000, allocator)) {
            try (VectorSchemaRoot b1 = buildBatch(allocator, new String[] {"a"}, new long[] {1})) {
                writeBuffer.append(b1); // threshold is 1 row -> auto-flush -> version 1
            }
            try (VectorSchemaRoot b2 = buildBatch(allocator, new String[] {"b"}, new long[] {2})) {
                writeBuffer.append(b2); // -> version 2
            }

            Snapshot snapshot = new SnapshotManager(tableRoot).latest();
            assertEquals(2, snapshot.getVersion());
            assertEquals(2, snapshot.getActiveFileCount());
        }
    }
}
