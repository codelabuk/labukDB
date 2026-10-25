package com.labuk.labukdb.iceberg;

import com.labuk.labukdb.core.decode.ParquetToArrowReader;
import com.labuk.labukdb.core.encode.ArrowToParquetWriter;
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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
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
 * Proves {@link IcebergTableFormat} against a real remote (MinIO) Iceberg
 * table — the same self-built-fixture pattern {@code IcebergTableFormatTest}
 * already uses locally (no Spark/Trino/Flink available to write a real
 * one), redirected at a live cluster. {@code IcebergTableFormat}'s
 * constructor already takes an arbitrary location string + Hadoop
 * {@code Configuration} — structurally remote-capable via
 * {@code HadoopTables}/Hadoop's own {@code FileSystem} resolution — but
 * never actually run against a live remote cluster before this.
 *
 * <b>Iceberg stores absolute file paths</b> (confirmed from the local
 * test's own fixture-building: {@code .withPath(parquetFile.getAbsolutePath())}),
 * unlike Delta's relative ones — so the committed {@code DataFile}'s path
 * here is the complete {@code s3a://} URI directly, not something resolved
 * against a table root.
 *
 * Opt-in by design, same env-var gate as every other MinIO integration
 * test this session.
 */
class IcebergRemoteMinioIntegrationTest {

    private static final org.apache.iceberg.Schema ICEBERG_SCHEMA = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "service", Types.StringType.get()),
            Types.NestedField.required(2, "amount", Types.LongType.get()),
            Types.NestedField.required(3, "level", Types.StringType.get()));

    private static final PartitionSpec SPEC = PartitionSpec.builderFor(ICEBERG_SCHEMA)
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
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MINIO_ENDPOINT not set — skipping live MinIO Iceberg test");
        assumeTrue(accessKey != null && !accessKey.isBlank(), "MINIO_ACCESS_KEY not set — skipping live MinIO Iceberg test");
        assumeTrue(secretKey != null && !secretKey.isBlank(), "MINIO_SECRET_KEY not set — skipping live MinIO Iceberg test");

        String bucket = System.getenv("MINIO_TEST_BUCKET");
        if (bucket == null || bucket.isBlank()) {
            bucket = "warehouse";
        }

        allocator = new RootAllocator(Long.MAX_VALUE);
        conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
        testRootUri = "s3a://" + bucket + "/labukdb-iceberg-remote-test/" + UUID.randomUUID();
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

    /** Writes one real Parquet file directly to MinIO and commits it to a fresh remote Iceberg table. */
    private Table buildRemoteFixtureTable() throws Exception {
        HadoopTables tables = new HadoopTables(conf);
        Table table = tables.create(ICEBERG_SCHEMA, SPEC, SortOrder.unsorted(), Map.of(), testRootUri);

        Field serviceField = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field amountField = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        try (VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(serviceField, amountField)), allocator)) {
            batch.allocateNew();
            VarCharVector serviceVector = (VarCharVector) batch.getVector("service");
            BigIntVector amountVector = (BigIntVector) batch.getVector("amount");
            String[] services = {"auth-svc", "payments-svc", "gateway"};
            for (int i = 0; i < services.length; i++) {
                serviceVector.setSafe(i, services[i].getBytes(StandardCharsets.UTF_8));
                amountVector.setSafe(i, i + 1L);
            }
            batch.setRowCount(services.length);

            String remoteDataFileUri = testRootUri + "/level=ERROR/part-0.parquet";
            TableLocation remoteDataFile = TableLocation.forRemote(remoteDataFileUri, conf);
            new ArrowToParquetWriter().write(remoteDataFile, batch); // the EXISTING, unmodified remote-capable encoder

            long sizeBytes = fileSystem.getFileStatus(new Path(remoteDataFileUri)).getLen();
            DataFile dataFile = DataFiles.builder(SPEC)
                    .withPath(remoteDataFileUri)
                    .withFileSizeInBytes(sizeBytes)
                    .withRecordCount(services.length)
                    .withPartitionPath("level=ERROR")
                    .build();
            table.newAppend().appendFile(dataFile).commit();
        }
        return table;
    }

    @Test
    void latest_realMinioBackedIcebergTable_resolvesCommittedFileWithCorrectPartitionValues() throws Exception {
        buildRemoteFixtureTable();

        NormalizedSnapshot snapshot = new IcebergTableFormat(testRootUri, conf).latest();

        assertEquals(List.of("level"), snapshot.getPartitionColumns());
        assertEquals(1, snapshot.getFiles().size());

        DataFileRef file = snapshot.getFiles().get(0);
        assertEquals("ERROR", file.getPartitionValues().get("level"));
        assertTrue(file.getPath().endsWith("part-0.parquet"));
        assertTrue(file.getSizeBytes() > 0);
    }

    @Test
    void latest_realMinioBackedIcebergTable_committedFileIsReadableByTheExistingUnmodifiedReader() throws Exception {
        buildRemoteFixtureTable();

        NormalizedSnapshot snapshot = new IcebergTableFormat(testRootUri, conf).latest();
        DataFileRef committed = snapshot.getFiles().get(0);

        // Iceberg's own path is already a complete s3a:// URI -- resolved
        // directly, not joined against a table root, same as the local
        // test resolves it as an already-absolute local path.
        TableLocation dataFile = TableLocation.forRemote(committed.getPath(), conf);
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
