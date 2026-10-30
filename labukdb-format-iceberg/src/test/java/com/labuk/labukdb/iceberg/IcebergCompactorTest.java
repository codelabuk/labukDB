package com.labuk.labukdb.iceberg;

import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import com.labuk.labukdb.core.format.DataFileRef;
import com.labuk.labukdb.core.format.NormalizedSnapshot;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The Iceberg counterpart of {@code core.log.CompactorTest} — same
 * service/amount/level schema, same two shapes: bin-pack several active
 * files in one partition into one, and a fewer-than-2-files no-op.
 */
class IcebergCompactorTest {

    private static final org.apache.iceberg.Schema ICEBERG_SCHEMA = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "service", Types.StringType.get()),
            Types.NestedField.required(2, "amount", Types.LongType.get()),
            Types.NestedField.required(3, "level", Types.StringType.get()));

    private static final PartitionSpec PARTITIONED_SPEC = PartitionSpec.builderFor(ICEBERG_SCHEMA)
            .identity("level")
            .build();

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
        new HadoopTables(new Configuration())
                .create(ICEBERG_SCHEMA, PARTITIONED_SPEC, SortOrder.unsorted(), Map.of(), tableRoot.toString());

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            IcebergTableWriter writer = new IcebergTableWriter(TableLocation.forLocal(tableRoot));
            try (VectorSchemaRoot b1 = buildBatch(allocator, new String[] {"a"}, new long[] {1})) {
                writer.appendBatch(b1, Map.of("level", "ERROR"));
            }
            try (VectorSchemaRoot b2 = buildBatch(allocator, new String[] {"b"}, new long[] {2})) {
                writer.appendBatch(b2, Map.of("level", "ERROR"));
            }
            try (VectorSchemaRoot b3 = buildBatch(allocator, new String[] {"c"}, new long[] {3})) {
                writer.appendBatch(b3, Map.of("level", "ERROR"));
            }

            NormalizedSnapshot before = new IcebergTableFormat(tableRoot.toString(), new Configuration()).latest();
            assertEquals(3, before.getFiles().size());
            List<String> originalPaths = new ArrayList<>();
            for (DataFileRef f : before.getFiles()) {
                originalPaths.add(f.getPath());
            }

            long newSnapshotId = new IcebergCompactor().compact(TableLocation.forLocal(tableRoot), Map.of("level", "ERROR"), allocator);
            assertNotEquals(-1, newSnapshotId, "3 active files -- a real compaction must happen");

            NormalizedSnapshot after = new IcebergTableFormat(tableRoot.toString(), new Configuration()).latest();
            assertEquals(1, after.getFiles().size(), "3 files compacted into exactly 1");

            // The 3 original files still physically exist -- compaction removes
            // them from the live snapshot, not from disk; that's Vacuum's job.
            for (String path : originalPaths) {
                assertTrue(Files.exists(java.nio.file.Path.of(path)), "original file must survive compaction: " + path);
            }

            DataFileRef compacted = after.getFiles().get(0);
            try (VectorSchemaRoot readBack = new ParquetToArrowReader().read(
                    new java.io.File(compacted.getPath()), allocator, List.of("service", "amount"), null, Map.of())) {
                assertEquals(3, readBack.getRowCount(), "merged file contains the union of all 3 original batches");
                VarCharVector serviceOut = (VarCharVector) readBack.getVector("service");
                List<String> services = new ArrayList<>();
                for (int i = 0; i < readBack.getRowCount(); i++) {
                    services.add(new String(serviceOut.get(i), StandardCharsets.UTF_8));
                }
                assertEquals(Set.of("a", "b", "c"), Set.copyOf(services));
            }
        }
    }

    @Test
    void compact_fewerThanTwoActiveFiles_isNoOp(@TempDir Path tableRoot) throws Exception {
        new HadoopTables(new Configuration())
                .create(ICEBERG_SCHEMA, PARTITIONED_SPEC, SortOrder.unsorted(), Map.of(), tableRoot.toString());

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            IcebergTableWriter writer = new IcebergTableWriter(TableLocation.forLocal(tableRoot));
            try (VectorSchemaRoot b1 = buildBatch(allocator, new String[] {"a"}, new long[] {1})) {
                writer.appendBatch(b1, Map.of("level", "ERROR"));
            }

            long result = new IcebergCompactor().compact(TableLocation.forLocal(tableRoot), Map.of("level", "ERROR"), allocator);
            assertEquals(-1, result, "only one active file -- nothing meaningful to compact");

            NormalizedSnapshot after = new IcebergTableFormat(tableRoot.toString(), new Configuration()).latest();
            assertEquals(1, after.getFiles().size(), "no compaction commit was made");
        }
    }
}
