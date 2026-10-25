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
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Proves {@link IcebergTableWriter} genuinely appends to and commits a
 * real Iceberg table — a real {@code HadoopTables.create} table (no
 * initial commit, unlike {@code IcebergTableFormatTest}'s fixture, since
 * this class is what's under test for writing the first file), read back
 * with the <b>existing, unmodified</b> {@link IcebergTableFormat}/{@link
 * ParquetToArrowReader}, proving the committed file is genuinely
 * queryable through the normal read path, not just present on disk.
 */
class IcebergTableWriterTest {

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
        VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(serviceField, amountField)), allocator);
        batch.allocateNew();
        VarCharVector serviceVector = (VarCharVector) batch.getVector("service");
        BigIntVector amountVector = (BigIntVector) batch.getVector("amount");
        for (int i = 0; i < services.length; i++) {
            serviceVector.setSafe(i, services[i].getBytes(StandardCharsets.UTF_8));
            amountVector.setSafe(i, amounts[i]);
        }
        batch.setRowCount(services.length);
        return batch;
    }

    @Test
    void appendBatch_writesAndCommitsAReadableFile(@TempDir Path tableRoot) throws Exception {
        new HadoopTables(new Configuration())
                .create(ICEBERG_SCHEMA, PARTITIONED_SPEC, SortOrder.unsorted(), Map.of(), tableRoot.toString());

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = buildBatch(allocator, new String[] {"auth-svc", "gateway"}, new long[] {1, 2})) {
            new IcebergTableWriter(TableLocation.forLocal(tableRoot)).appendBatch(batch, Map.of("level", "ERROR"));
        }

        NormalizedSnapshot snapshot = new IcebergTableFormat(tableRoot.toString(), new Configuration()).latest();
        assertEquals(1, snapshot.getFiles().size());
        DataFileRef file = snapshot.getFiles().get(0);
        assertEquals("ERROR", file.getPartitionValues().get("level"));
        assertTrue(file.getPath().contains("level=ERROR"));

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot readBack = new ParquetToArrowReader().read(
                     new java.io.File(file.getPath()), allocator, List.of("service", "amount"), null, Map.of())) {
            assertEquals(2, readBack.getRowCount());
            VarCharVector readService = (VarCharVector) readBack.getVector("service");
            assertEquals("auth-svc", new String(readService.get(0), StandardCharsets.UTF_8));
            assertEquals("gateway", new String(readService.get(1), StandardCharsets.UTF_8));
        }
    }

    @Test
    void appendBatch_calledTwice_bothCommitsVisibleInLatest(@TempDir Path tableRoot) throws Exception {
        new HadoopTables(new Configuration())
                .create(ICEBERG_SCHEMA, PARTITIONED_SPEC, SortOrder.unsorted(), Map.of(), tableRoot.toString());

        IcebergTableWriter writer = new IcebergTableWriter(TableLocation.forLocal(tableRoot));
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot errorBatch = buildBatch(allocator, new String[] {"auth-svc"}, new long[] {1});
             VectorSchemaRoot infoBatch = buildBatch(allocator, new String[] {"gateway"}, new long[] {2})) {
            writer.appendBatch(errorBatch, Map.of("level", "ERROR"));
            writer.appendBatch(infoBatch, Map.of("level", "INFO"));
        }

        NormalizedSnapshot snapshot = new IcebergTableFormat(tableRoot.toString(), new Configuration()).latest();
        assertEquals(2, snapshot.getFiles().size(), "both separately committed files must be visible");
        List<String> levels = snapshot.getFiles().stream()
                .map(f -> f.getPartitionValues().get("level")).sorted().toList();
        assertEquals(List.of("ERROR", "INFO"), levels);
    }

    @Test
    void appendBatch_unpartitionedTable_succeeds(@TempDir Path tableRoot) throws Exception {
        // Resolves the one real edge case flagged during design: does
        // DataFiles.Builder#withPartitionPath("") work for an
        // unpartitioned spec, not assumed from memory.
        PartitionSpec unpartitioned = PartitionSpec.unpartitioned();
        new HadoopTables(new Configuration())
                .create(ICEBERG_SCHEMA, unpartitioned, SortOrder.unsorted(), Map.of(), tableRoot.toString());

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = buildBatch(allocator, new String[] {"auth-svc"}, new long[] {1})) {
            new IcebergTableWriter(TableLocation.forLocal(tableRoot)).appendBatch(batch, Map.of());
        }

        NormalizedSnapshot snapshot = new IcebergTableFormat(tableRoot.toString(), new Configuration()).latest();
        assertEquals(1, snapshot.getFiles().size());
        assertTrue(snapshot.getFiles().get(0).getPartitionValues().isEmpty());
    }
}
