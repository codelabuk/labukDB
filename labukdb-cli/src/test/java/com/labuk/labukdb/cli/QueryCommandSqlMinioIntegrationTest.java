package com.labuk.labukdb.cli;

import com.labuk.labukdb.core.catalog.TableCatalog;
import com.labuk.labukdb.core.log.CommitWriter;
import com.labuk.labukdb.core.log.TableCreator;
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
 * Closes the one specific gap the correctness audit found in the SQL query
 * layer: {@link QueryCommandSqlIntegrationTest} proves {@code --sql}
 * combines correctly across multiple local files, and separately
 * {@code FlightRemoteAggregationMinioIntegrationTest} proves the same
 * combine executor against real MinIO — but through Flight's {@code DoGet},
 * never through the CLI's {@code --sql} entry point directly. This is the
 * first test that actually issues a literal {@code --sql} command against a
 * MinIO-backed table, not an inferred-safe composition of two separately
 * tested paths.
 *
 * Opt-in, same {@code MINIO_*} env-var gate as every other MinIO
 * integration test in this project; also isolates the real {@code
 * TableCatalog} the same way {@link QueryCommandSqlIntegrationTest} does,
 * by pointing {@code user.home} at a fresh {@code @TempDir}.
 */
class QueryCommandSqlMinioIntegrationTest {

    private FileSystem fileSystem;
    private Path testRoot;
    private Configuration conf;
    private String testRootUri;
    private String originalUserHome;
    private PrintStream originalOut;
    private ByteArrayOutputStream capturedOut;

    @BeforeEach
    void checkPreconditionsAndConnect(@TempDir java.nio.file.Path fakeHome) throws Exception {
        // Capture/swap restorable state BEFORE any assumeTrue check that
        // might abort this method early -- found the hard way: cleanUp()
        // unconditionally restores these, and when a skipped assumption
        // left them never assigned, System.setProperty("user.home", null)
        // throws NullPointerException instead of skipping cleanly.
        originalUserHome = System.getProperty("user.home");
        System.setProperty("user.home", fakeHome.toString());
        originalOut = System.out;
        capturedOut = new ByteArrayOutputStream();
        System.setOut(new PrintStream(capturedOut, true, StandardCharsets.UTF_8));

        String endpoint = System.getenv("MINIO_ENDPOINT");
        String accessKey = System.getenv("MINIO_ACCESS_KEY");
        String secretKey = System.getenv("MINIO_SECRET_KEY");
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MINIO_ENDPOINT not set — skipping live MinIO CLI SQL test");
        assumeTrue(accessKey != null && !accessKey.isBlank(), "MINIO_ACCESS_KEY not set — skipping live MinIO CLI SQL test");
        assumeTrue(secretKey != null && !secretKey.isBlank(), "MINIO_SECRET_KEY not set — skipping live MinIO CLI SQL test");

        String bucket = System.getenv("MINIO_TEST_BUCKET");
        if (bucket == null || bucket.isBlank()) {
            bucket = "warehouse";
        }

        conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
        testRootUri = "s3a://" + bucket + "/labukdb-cli-sql-test/" + UUID.randomUUID();
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

    /** ERROR: amounts 1..5 (sum 15, count 5). INFO: amounts 100..104 (count 5). Two separate commits -> two files. */
    private void buildTwoPartitionFixture() throws Exception {
        TableLocation location = TableLocation.forRemote(testRootUri, conf);
        Field amount = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field message = new Field("message", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Schema schema = new Schema(List.of(amount, message));
        new TableCreator().createTable(location, schema, List.of("level"), null);

        CommitWriter writer = CommitWriter.forLocationSingleWriterOnly(location);
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            try (VectorSchemaRoot errorBatch = buildBatch(allocator, schema, 1, 5, "error")) {
                writer.appendBatch(errorBatch, Map.of("level", "ERROR"));
            }
            try (VectorSchemaRoot infoBatch = buildBatch(allocator, schema, 100, 5, "info")) {
                writer.appendBatch(infoBatch, Map.of("level", "INFO"));
            }
        }
    }

    private VectorSchemaRoot buildBatch(BufferAllocator allocator, Schema schema, int startAmount, int count, String messagePrefix) {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        BigIntVector amountVector = (BigIntVector) root.getVector("amount");
        VarCharVector messageVector = (VarCharVector) root.getVector("message");
        for (int i = 0; i < count; i++) {
            long amount = startAmount + i;
            amountVector.setSafe(i, amount);
            messageVector.setSafe(i, (messagePrefix + "-msg-" + amount).getBytes(StandardCharsets.UTF_8));
        }
        root.setRowCount(count);
        return root;
    }

    @Test
    void sqlGroupByCount_againstRealMinioBackedTable_combinesAcrossFiles() throws Exception {
        buildTwoPartitionFixture();
        TableCatalog.atDefaultLocation().register("logs", testRootUri);

        QueryCommand.run(new String[] {"--sql",
                "SELECT level, COUNT(amount) AS cnt FROM logs GROUP BY level"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("level | cnt"), output);
        assertTrue(output.contains("ERROR | 5"), "ERROR partition's 5 rows must combine into one count, from real MinIO: " + output);
        assertTrue(output.contains("INFO | 5"), "INFO partition's 5 rows must combine into one count, from real MinIO: " + output);
        assertTrue(output.contains("2 row(s)"), output);
    }

    @Test
    void sqlOrderByLimit_againstRealMinioBackedTable_returnsGlobalTopN() throws Exception {
        buildTwoPartitionFixture();
        TableCatalog.atDefaultLocation().register("logs", testRootUri);

        QueryCommand.run(new String[] {"--sql",
                "SELECT amount FROM logs ORDER BY amount DESC LIMIT 3"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        List<String> dataLines = output.lines()
                .dropWhile(line -> !line.startsWith("---"))
                .skip(1)
                .takeWhile(line -> !line.isBlank())
                .toList();
        assertEquals(List.of("104", "103", "102"), dataLines,
                "must list the true global top-3 across both real-MinIO files, not a per-file-local one: " + output);
        assertTrue(output.contains("3 row(s)"), output);
    }

    @Test
    void sqlWithWhere_againstRealMinioBackedTable_filtersCorrectly() throws Exception {
        buildTwoPartitionFixture();
        TableCatalog.atDefaultLocation().register("logs", testRootUri);

        QueryCommand.run(new String[] {"--sql",
                "SELECT amount, message FROM logs WHERE level = 'ERROR'"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("5 row(s)"), "only the ERROR partition's 5 rows should match, from real MinIO: " + output);
        assertFalse(output.contains("info-msg"), "INFO partition rows must be excluded by the WHERE clause: " + output);
    }
}
