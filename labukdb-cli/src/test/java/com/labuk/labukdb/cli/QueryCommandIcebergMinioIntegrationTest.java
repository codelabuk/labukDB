package com.labuk.labukdb.cli;

import com.labuk.labukdb.core.catalog.TableCatalog;
import com.labuk.labukdb.core.encode.ArrowToParquetWriter;
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
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Proves the one combination never previously exercised by a test: an
 * Iceberg table, backed by real MinIO, reached through the CLI's
 * {@code --sql} entry point (GROUP BY specifically). By code inspection,
 * {@code QueryCommand}'s Delta-vs-Iceberg and local-vs-remote dispatch
 * are orthogonal ({@code TableResolver}/{@code DetectedFormat} decide
 * format purely from {@code TableLocation} content, independent of
 * remote-vs-local) — so this should already work; this test is that
 * proof, not new production code.
 *
 * Combines {@code QueryCommandIcebergIntegrationTest}'s two-partition
 * Iceberg/GROUP BY fixture shape with {@code
 * IcebergRemoteMinioIntegrationTest}'s remote-write substitution (real
 * {@code TableLocation.forRemote}-based {@code ArrowToParquetWriter}
 * write, full {@code s3a://} URI as the committed {@code DataFile}'s path).
 *
 * Opt-in, same {@code MINIO_*} env-var gate as every other MinIO
 * integration test in this project; isolates the real {@code
 * TableCatalog} the same way {@code QueryCommandSqlMinioIntegrationTest}
 * does, by pointing {@code user.home} at a fresh {@code @TempDir}.
 */
class QueryCommandIcebergMinioIntegrationTest {

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
    private String originalUserHome;
    private PrintStream originalOut;
    private ByteArrayOutputStream capturedOut;

    @BeforeEach
    void checkPreconditionsAndConnect(@TempDir java.nio.file.Path fakeHome) throws Exception {
        // Capture/swap restorable state BEFORE any assumeTrue check, same
        // ordering QueryCommandSqlMinioIntegrationTest already settled on --
        // cleanUp() unconditionally restores these, and a skipped assumption
        // leaving them unassigned would NPE on restore instead of skipping cleanly.
        originalUserHome = System.getProperty("user.home");
        System.setProperty("user.home", fakeHome.toString());
        originalOut = System.out;
        capturedOut = new ByteArrayOutputStream();
        System.setOut(new PrintStream(capturedOut, true, StandardCharsets.UTF_8));

        String endpoint = System.getenv("MINIO_ENDPOINT");
        String accessKey = System.getenv("MINIO_ACCESS_KEY");
        String secretKey = System.getenv("MINIO_SECRET_KEY");
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MINIO_ENDPOINT not set — skipping live MinIO CLI Iceberg SQL test");
        assumeTrue(accessKey != null && !accessKey.isBlank(), "MINIO_ACCESS_KEY not set — skipping live MinIO CLI Iceberg SQL test");
        assumeTrue(secretKey != null && !secretKey.isBlank(), "MINIO_SECRET_KEY not set — skipping live MinIO CLI Iceberg SQL test");

        String bucket = System.getenv("MINIO_TEST_BUCKET");
        if (bucket == null || bucket.isBlank()) {
            bucket = "warehouse";
        }

        conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
        testRootUri = "s3a://" + bucket + "/labukdb-cli-iceberg-minio-test/" + UUID.randomUUID();
        testRoot = new Path(testRootUri);
        fileSystem = testRoot.getFileSystem(conf);
    }

    @AfterEach
    void cleanUp() throws Exception {
        System.setOut(originalOut);
        System.setProperty("user.home", originalUserHome);
        if (fileSystem != null && testRoot != null) {
            fileSystem.delete(testRoot, true);
        }
    }

    private void writeRemotePartitionFile(Table table, String level, int startAmount, int count) throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            Field serviceField = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
            Field amountField = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
            try (VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(serviceField, amountField)), allocator)) {
                batch.allocateNew();
                VarCharVector serviceVector = (VarCharVector) batch.getVector("service");
                BigIntVector amountVector = (BigIntVector) batch.getVector("amount");
                for (int i = 0; i < count; i++) {
                    serviceVector.setSafe(i, (level.toLowerCase() + "-svc").getBytes(StandardCharsets.UTF_8));
                    amountVector.setSafe(i, (long) (startAmount + i));
                }
                batch.setRowCount(count);

                String remoteDataFileUri = testRootUri + "/level=" + level + "/part-0.parquet";
                new ArrowToParquetWriter().write(TableLocation.forRemote(remoteDataFileUri, conf), batch);

                long sizeBytes = fileSystem.getFileStatus(new Path(remoteDataFileUri)).getLen();
                DataFile dataFile = DataFiles.builder(SPEC)
                        .withPath(remoteDataFileUri)
                        .withFileSizeInBytes(sizeBytes)
                        .withRecordCount(count)
                        .withPartitionPath("level=" + level)
                        .build();
                table.newAppend().appendFile(dataFile).commit();
            }
        }
    }

    /** ERROR: amounts 1..5 (sum 15). INFO: amounts 100..104 (sum 510). Mirrors QueryCommandIcebergIntegrationTest's local fixture. */
    private void buildTwoPartitionFixture() throws Exception {
        Table table = new HadoopTables(conf).create(ICEBERG_SCHEMA, SPEC, SortOrder.unsorted(), Map.of(), testRootUri);
        writeRemotePartitionFile(table, "ERROR", 1, 5);
        writeRemotePartitionFile(table, "INFO", 100, 5);
    }

    @Test
    void sqlGroupByCount_againstRealMinioBackedIcebergTable_combinesAcrossFiles() throws Exception {
        buildTwoPartitionFixture();
        TableCatalog.atDefaultLocation().register("iceberg_logs_minio", testRootUri);

        QueryCommand.run(new String[] {"--sql",
                "SELECT level, COUNT(amount) AS cnt FROM iceberg_logs_minio GROUP BY level"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("level | cnt"), output);
        assertTrue(output.contains("ERROR | 5"), "ERROR partition's 5 rows must combine into one count, from real MinIO: " + output);
        assertTrue(output.contains("INFO | 5"), "INFO partition's 5 rows must combine into one count, from real MinIO: " + output);
        assertTrue(output.contains("2 row(s)"), output);
    }
}
