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
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class CompactorTest {

    private void writeInitialCommit(Path tableRoot) throws Exception {
        Path logDir = tableRoot.resolve("_delta_log");
        Files.createDirectories(logDir);
        String schemaString = "{\"type\":\"struct\",\"fields\":["
                + "{\"name\":\"service\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},"
                + "{\"name\":\"amount\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}";
        String commit = String.join("\n",
                "{\"commitInfo\":{\"timestamp\":1732000000000,\"operation\":\"CREATE TABLE\"}}",
                "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}",
                "{\"metaData\":{\"id\":\"compactor-test-table\",\"format\":{\"provider\":\"parquet\",\"options\":{}},"
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
        VarCharVector sv = (VarCharVector) root.getVector("service");
        BigIntVector av = (BigIntVector) root.getVector("amount");
        for (int i = 0; i < services.length; i++) {
            sv.setSafe(i, services[i].getBytes(StandardCharsets.UTF_8));
            av.setSafe(i, amounts[i]);
        }
        root.setRowCount(services.length);
        return root;
    }

    @Test
    void compact_mergesAllActiveFilesForPartition_intoOne(@TempDir Path tableRoot) throws Exception {
        writeInitialCommit(tableRoot);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            CommitWriter writer = new CommitWriter(tableRoot);
            try (VectorSchemaRoot b1 = buildBatch(allocator, new String[] {"a"}, new long[] {1})) {
                writer.appendBatch(b1, Map.of("level", "ERROR"));
            }
            try (VectorSchemaRoot b2 = buildBatch(allocator, new String[] {"b"}, new long[] {2})) {
                writer.appendBatch(b2, Map.of("level", "ERROR"));
            }
            try (VectorSchemaRoot b3 = buildBatch(allocator, new String[] {"c"}, new long[] {3})) {
                writer.appendBatch(b3, Map.of("level", "ERROR"));
            }

            Snapshot before = new SnapshotManager(tableRoot).latest();
            assertEquals(3, before.getActiveFileCount());
            List<String> originalPaths = new ArrayList<>();
            for (AddFile f : before.getActiveFiles()) {
                originalPaths.add(f.getPath());
            }

            long newVersion = new Compactor().compact(tableRoot, Map.of("level", "ERROR"), allocator);
            assertEquals(4, newVersion); // v0 create + 3 appends (v1-v3) + compaction = v4

            Snapshot after = new SnapshotManager(tableRoot).latest();
            assertEquals(1, after.getActiveFileCount(), "3 files compacted into exactly 1");

            // The 3 original files still physically exist -- compaction removes
            // them from the log, not from disk; that's Vacuum's job.
            for (String path : originalPaths) {
                assertTrue(Files.exists(tableRoot.resolve(path)), "original file must survive compaction: " + path);
            }

            AddFile compacted = after.getActiveFiles().iterator().next();
            File compactedFile = tableRoot.resolve(compacted.getPath()).toFile();
            try (VectorSchemaRoot readBack = new ParquetToArrowReader().read(
                    compactedFile, allocator, List.of("service", "amount"), null, Map.of())) {
                assertEquals(3, readBack.getRowCount(), "merged file contains the union of all 3 original batches");
                VarCharVector serviceOut = (VarCharVector) readBack.getVector("service");
                List<String> services = new ArrayList<>();
                for (int i = 0; i < readBack.getRowCount(); i++) {
                    services.add(new String(serviceOut.get(i), StandardCharsets.UTF_8));
                }
                // Order isn't guaranteed -- Snapshot.getActiveFiles() is backed by a
                // HashMap, so which of the 3 source files gets read first isn't
                // specified. The union of rows is what matters.
                assertEquals(Set.of("a", "b", "c"), Set.copyOf(services));
            }
        }
    }

    @Test
    void compact_fewerThanTwoActiveFiles_isNoOp(@TempDir Path tableRoot) throws Exception {
        writeInitialCommit(tableRoot);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            CommitWriter writer = new CommitWriter(tableRoot);
            try (VectorSchemaRoot b1 = buildBatch(allocator, new String[] {"a"}, new long[] {1})) {
                writer.appendBatch(b1, Map.of("level", "ERROR"));
            }

            long result = new Compactor().compact(tableRoot, Map.of("level", "ERROR"), allocator);
            assertEquals(-1, result, "only one active file -- nothing meaningful to compact");
            assertEquals(1, new SnapshotManager(tableRoot).latest().getVersion(), "no extra commit was made");
        }
    }
}
