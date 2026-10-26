package com.labuk.labukdb.iceberg;

import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import com.labuk.labukdb.core.format.DataFileRef;
import com.labuk.labukdb.core.format.NormalizedSnapshot;
import com.labuk.labukdb.core.storage.MinioConfiguration;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Proves {@link IcebergTableWriter} genuinely appends to and commits a real
 * remote (MinIO) Iceberg table — the write-side counterpart to {@link
 * IcebergRemoteMinioIntegrationTest}, which only ever exercised {@link
 * IcebergTableFormat} (reads) against a live cluster. {@code
 * IcebergTableWriter}'s constructor already dispatches on {@code
 * TableLocation.isRemote()} as its unmodified local test proves
 * ({@code IcebergTableWriterTest}) — this closes the one real coverage gap
 * found during the correctness audit: no MinIO test had ever actually
 * called {@code appendBatch}.
 *
 * Same opt-in env-var gate, fixture-build, and cleanup pattern as every
 * other MinIO integration test in this project.
 */
class IcebergWriterRemoteMinioIntegrationTest {

    private static final org.apache.iceberg.Schema ICEBERG_SCHEMA = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "service", Types.StringType.get()),
            Types.NestedField.required(2, "amount", Types.LongType.get()),
            Types.NestedField.required(3, "level", Types.StringType.get()));

    private static final PartitionSpec PARTITIONED_SPEC = PartitionSpec.builderFor(ICEBERG_SCHEMA)
            .identity("level")
            .build();

    private FileSystem fileSystem;
    private Path testRoot;
    private Configuration conf;
    private String testRootUri;
    private BufferAllocator allocator;

    @BeforeEach
    void checkPreconditionsAndConnect() throws Exception {
        String endpoint = System.getenv("MINIO_ENDPOINT");
        String accessKey = System.getenv("MINIO_ACCESS_KEY");
        String secretKey = System.getenv("MINIO_SECRET_KEY");
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MINIO_ENDPOINT not set — skipping live MinIO Iceberg write test");
        assumeTrue(accessKey != null && !accessKey.isBlank(), "MINIO_ACCESS_KEY not set — skipping live MinIO Iceberg write test");
        assumeTrue(secretKey != null && !secretKey.isBlank(), "MINIO_SECRET_KEY not set — skipping live MinIO Iceberg write test");

        String bucket = System.getenv("MINIO_TEST_BUCKET");
        if (bucket == null || bucket.isBlank()) {
            bucket = "warehouse";
        }

        allocator = new RootAllocator(Long.MAX_VALUE);
        conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
        testRootUri = "s3a://" + bucket + "/labukdb-iceberg-writer-remote-test/" + UUID.randomUUID();
        testRoot = new Path(testRootUri);
        fileSystem = testRoot.getFileSystem(conf);
    }

    @AfterEach
    void cleanUp() throws Exception {
        if (fileSystem != null && testRoot != null) {
            fileSystem.delete(testRoot, true);
        }
        if (allocator != null) {
            allocator.close();
        }
    }

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
    void appendBatch_toRealMinioBackedTable_commitsAReadableFile() throws Exception {
        new HadoopTables(conf).create(ICEBERG_SCHEMA, PARTITIONED_SPEC, SortOrder.unsorted(), Map.of(), testRootUri);

        try (VectorSchemaRoot batch = buildBatch(allocator, new String[] {"auth-svc", "gateway"}, new long[] {1, 2})) {
            new IcebergTableWriter(TableLocation.forRemote(testRootUri, conf))
                    .appendBatch(batch, Map.of("level", "ERROR"));
        }

        NormalizedSnapshot snapshot = new IcebergTableFormat(testRootUri, conf).latest();
        assertEquals(1, snapshot.getFiles().size());
        DataFileRef file = snapshot.getFiles().get(0);
        assertEquals("ERROR", file.getPartitionValues().get("level"));
        assertTrue(file.getPath().contains("level=ERROR"));
        assertTrue(file.getPath().startsWith("s3a://"), "Iceberg's own stored path must be the full remote URI");

        TableLocation committedFile = TableLocation.forRemote(file.getPath(), conf);
        try (VectorSchemaRoot readBack = new ParquetToArrowReader().read(
                committedFile, allocator, List.of("service", "amount"), null, Map.of())) {
            assertEquals(2, readBack.getRowCount());
            VarCharVector readService = (VarCharVector) readBack.getVector("service");
            assertEquals("auth-svc", new String(readService.get(0), StandardCharsets.UTF_8));
            assertEquals("gateway", new String(readService.get(1), StandardCharsets.UTF_8));
        }
    }

    @Test
    void appendBatch_calledTwiceAgainstMinio_bothCommitsVisibleInLatest() throws Exception {
        new HadoopTables(conf).create(ICEBERG_SCHEMA, PARTITIONED_SPEC, SortOrder.unsorted(), Map.of(), testRootUri);

        IcebergTableWriter writer = new IcebergTableWriter(TableLocation.forRemote(testRootUri, conf));
        try (VectorSchemaRoot errorBatch = buildBatch(allocator, new String[] {"auth-svc"}, new long[] {1});
             VectorSchemaRoot infoBatch = buildBatch(allocator, new String[] {"gateway"}, new long[] {2})) {
            writer.appendBatch(errorBatch, Map.of("level", "ERROR"));
            writer.appendBatch(infoBatch, Map.of("level", "INFO"));
        }

        NormalizedSnapshot snapshot = new IcebergTableFormat(testRootUri, conf).latest();
        assertEquals(2, snapshot.getFiles().size(), "both separately committed remote files must be visible");
        List<String> levels = snapshot.getFiles().stream()
                .map(f -> f.getPartitionValues().get("level")).sorted().toList();
        assertEquals(List.of("ERROR", "INFO"), levels);
    }
}
