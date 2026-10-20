package com.labuk.labukdb.iceberg;

import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import com.labuk.labukdb.core.encode.ArrowToParquetWriter;
import com.labuk.labukdb.core.format.DataFileRef;
import com.labuk.labukdb.core.format.NormalizedSnapshot;
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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Proves {@link IcebergTableFormat} against a real Iceberg table — built
 * with {@code iceberg-core}'s own table-creation API in a
 * {@code @TempDir}, the same self-built-fixture pattern
 * {@code CommitWriterTest}/{@code FlightIntegrationTest} already use for
 * Delta, since no Spark/Trino/Flink is available to write a real one
 * here. The actual payoff test — reading the committed file back with the
 * *existing, unmodified* {@code ParquetToArrowReader} — is what proves
 * the decode layer is genuinely format-agnostic, not just assumed to be.
 */
class IcebergTableFormatTest {

    private static final org.apache.iceberg.Schema ICEBERG_SCHEMA = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "service", Types.StringType.get()),
            Types.NestedField.required(2, "amount", Types.LongType.get()),
            Types.NestedField.required(3, "level", Types.StringType.get()));

    private static final PartitionSpec SPEC = PartitionSpec.builderFor(ICEBERG_SCHEMA)
            .identity("level")
            .build();

    /** Writes one real Parquet file (service, amount only — "level" is the partition column, reconstructed, not physical) and commits it to a fresh Iceberg table. */
    private Table buildFixtureTable(Path tableRoot) throws Exception {
        HadoopTables tables = new HadoopTables(new Configuration());
        Table table = tables.create(ICEBERG_SCHEMA, SPEC, SortOrder.unsorted(), Map.of(), tableRoot.toString());

        Field serviceField = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field amountField = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(serviceField, amountField)), allocator)) {
            batch.allocateNew();
            VarCharVector serviceVector = (VarCharVector) batch.getVector("service");
            BigIntVector amountVector = (BigIntVector) batch.getVector("amount");
            String[] services = {"auth-svc", "payments-svc", "gateway"};
            for (int i = 0; i < services.length; i++) {
                serviceVector.setSafe(i, services[i].getBytes(StandardCharsets.UTF_8));
                amountVector.setSafe(i, i + 1L);
            }
            batch.setRowCount(services.length);

            File parquetFile = tableRoot.resolve("level=ERROR/part-0.parquet").toFile();
            parquetFile.getParentFile().mkdirs();
            new ArrowToParquetWriter().write(parquetFile, batch); // the EXISTING, unmodified encoder

            DataFile dataFile = DataFiles.builder(SPEC)
                    .withPath(parquetFile.getAbsolutePath())
                    .withFileSizeInBytes(parquetFile.length())
                    .withRecordCount(services.length)
                    .withPartitionPath("level=ERROR")
                    .build();
            table.newAppend().appendFile(dataFile).commit();
        }
        return table;
    }

    @Test
    void latest_resolvesCommittedFileWithCorrectPartitionValues(@TempDir Path tableRoot) throws Exception {
        buildFixtureTable(tableRoot);

        NormalizedSnapshot snapshot = new IcebergTableFormat(tableRoot.toString(), new Configuration()).latest();

        assertEquals(List.of("level"), snapshot.getPartitionColumns());
        assertEquals(1, snapshot.getFiles().size());

        DataFileRef file = snapshot.getFiles().get(0);
        assertEquals("ERROR", file.getPartitionValues().get("level"));
        assertTrue(file.getPath().endsWith("part-0.parquet"));
        assertTrue(file.getSizeBytes() > 0);
    }

    @Test
    void latest_committedFile_isReadableByTheExistingUnmodifiedParquetToArrowReader(@TempDir Path tableRoot) throws Exception {
        buildFixtureTable(tableRoot);

        NormalizedSnapshot snapshot = new IcebergTableFormat(tableRoot.toString(), new Configuration()).latest();
        DataFileRef committed = snapshot.getFiles().get(0);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot readBack = new ParquetToArrowReader().read(
                     new File(committed.getPath()), allocator, List.of("service", "amount"), null, Map.of())) {
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

    @Test
    void atVersion_resolvesByRealSnapshotId_notASequentialIndex(@TempDir Path tableRoot) throws Exception {
        Table table = buildFixtureTable(tableRoot);
        long realSnapshotId = table.currentSnapshot().snapshotId();

        NormalizedSnapshot byId = new IcebergTableFormat(tableRoot.toString(), new Configuration()).atVersion(realSnapshotId);

        assertEquals(realSnapshotId, byId.getVersion());
        assertEquals(1, byId.getFiles().size());
    }
}
