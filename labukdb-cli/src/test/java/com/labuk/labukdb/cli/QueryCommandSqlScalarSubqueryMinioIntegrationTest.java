package com.labuk.labukdb.cli;

import com.labuk.labukdb.core.catalog.TableCatalog;
import com.labuk.labukdb.core.log.CommitWriter;
import com.labuk.labukdb.core.log.TableCreator;
import com.labuk.labukdb.core.storage.MinioConfiguration;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
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
 * Closes the one gap the scalar-subquery work left open: {@link
 * QueryCommandSqlScalarSubqueryIntegrationTest} proves correctness against
 * local fixtures only. Same opt-in {@code MINIO_*} env-var gate as every
 * other MinIO integration test in this project.
 */
class QueryCommandSqlScalarSubqueryMinioIntegrationTest {

    private FileSystem fileSystem;
    private Path testRoot;
    private Configuration conf;
    private String testRootUri;
    private BufferAllocator allocator;
    private String originalUserHome;
    private PrintStream originalOut;
    private ByteArrayOutputStream capturedOut;

    @BeforeEach
    void checkPreconditionsAndConnect(@TempDir java.nio.file.Path fakeHome) throws Exception {
        originalUserHome = System.getProperty("user.home");
        System.setProperty("user.home", fakeHome.toString());
        originalOut = System.out;
        capturedOut = new ByteArrayOutputStream();
        System.setOut(new PrintStream(capturedOut, true, StandardCharsets.UTF_8));

        String endpoint = System.getenv("MINIO_ENDPOINT");
        String accessKey = System.getenv("MINIO_ACCESS_KEY");
        String secretKey = System.getenv("MINIO_SECRET_KEY");
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MINIO_ENDPOINT not set -- skipping live MinIO scalar subquery test");
        assumeTrue(accessKey != null && !accessKey.isBlank(), "MINIO_ACCESS_KEY not set -- skipping live MinIO scalar subquery test");
        assumeTrue(secretKey != null && !secretKey.isBlank(), "MINIO_SECRET_KEY not set -- skipping live MinIO scalar subquery test");

        String bucket = System.getenv("MINIO_TEST_BUCKET");
        if (bucket == null || bucket.isBlank()) {
            bucket = "warehouse";
        }

        allocator = new RootAllocator(Long.MAX_VALUE);
        conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
        testRootUri = "s3a://" + bucket + "/labukdb-cli-scalar-subquery-minio-test/" + UUID.randomUUID();
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
        if (allocator != null) {
            allocator.close();
        }
    }

    /** orders (Delta, remote): id(long), amount(long). id 1,2,3 -- amounts 100, 150, 200. */
    private void buildOrders(String uri) throws Exception {
        Field id = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field amount = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Schema schema = new Schema(List.of(id, amount));
        TableLocation location = TableLocation.forRemote(uri, conf);
        new TableCreator().createTable(location, schema, List.of(), null);

        try (VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
            batch.allocateNew();
            BigIntVector idVector = (BigIntVector) batch.getVector("id");
            BigIntVector amountVector = (BigIntVector) batch.getVector("amount");
            long[] ids = {1, 2, 3};
            long[] amounts = {100, 150, 200};
            for (int i = 0; i < ids.length; i++) {
                idVector.setSafe(i, ids[i]);
                amountVector.setSafe(i, amounts[i]);
            }
            batch.setRowCount(ids.length);
            CommitWriter.forLocation(location).appendBatch(batch, Map.of());
        }
    }

    /** limits (Delta, remote): id(long), max_amount(long). Exactly one row: {1, 150}. */
    private void buildLimits(String uri) throws Exception {
        Field id = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field maxAmount = new Field("max_amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Schema schema = new Schema(List.of(id, maxAmount));
        TableLocation location = TableLocation.forRemote(uri, conf);
        new TableCreator().createTable(location, schema, List.of(), null);

        try (VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
            batch.allocateNew();
            ((BigIntVector) batch.getVector("id")).setSafe(0, 1L);
            ((BigIntVector) batch.getVector("max_amount")).setSafe(0, 150L);
            batch.setRowCount(1);
            CommitWriter.forLocation(location).appendBatch(batch, Map.of());
        }
    }

    @Test
    void plainColumnScalarSubquery_realMinioBackedDeltaTables_matchesExactlyOneRow() throws Exception {
        String ordersUri = testRootUri + "/orders";
        String limitsUri = testRootUri + "/limits";
        buildOrders(ordersUri);
        buildLimits(limitsUri);

        TableCatalog.atDefaultLocation().register("orders", ordersUri);
        TableCatalog.atDefaultLocation().register("limits", limitsUri);

        QueryCommand.run(new String[] {"--sql",
                "SELECT o.id FROM orders o WHERE o.amount = (SELECT l.max_amount FROM limits l)"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("1 row(s)"), output);
        List<String> dataLines = output.lines().dropWhile(l -> !l.startsWith("---")).skip(1)
                .takeWhile(l -> !l.isBlank()).toList();
        assertEquals(List.of("2"), dataLines, "only id=2 has amount=150, matching the single limits row, from real MinIO: " + output);
    }

    @Test
    void aggregateScalarSubquery_realMinioBackedDeltaTables_matchesTheMaxRow() throws Exception {
        String ordersUri = testRootUri + "/orders";
        buildOrders(ordersUri);

        TableCatalog.atDefaultLocation().register("orders", ordersUri);

        QueryCommand.run(new String[] {"--sql",
                "SELECT o.id FROM orders o WHERE o.amount = (SELECT MAX(o2.amount) AS m FROM orders o2)"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("1 row(s)"), output);
        List<String> dataLines = output.lines().dropWhile(l -> !l.startsWith("---")).skip(1)
                .takeWhile(l -> !l.isBlank()).toList();
        assertEquals(List.of("3"), dataLines, "id=3 has amount=200, the max, from real MinIO: " + output);
    }

    /** empty_orders (Delta, remote): id(long), amount(long). Created, but no rows ever written -- zero files in the log. */
    private void buildEmptyOrders(String uri) throws Exception {
        Field id = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field amount = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Schema schema = new Schema(List.of(id, amount));
        new TableCreator().createTable(TableLocation.forRemote(uri, conf), schema, List.of(), null);
    }

    /**
     * Named gap, now fixed, against real MinIO: a group-by-less aggregate
     * scalar subquery over zero matching files. {@code COUNT} of zero
     * rows is {@code 0}, not {@code NULL} -- a real, non-null value, so
     * {@code !=} against it is true for every order's {@code id} (1,2,3),
     * distinguishing a correct "0" from an incorrect NULL-short-circuit
     * (which would silently produce "0 row(s)" here instead).
     */
    @Test
    void countScalarSubquery_overZeroMatchingFiles_realMinioBackedDeltaTables_resolvesToZeroNotNull() throws Exception {
        String ordersUri = testRootUri + "/orders";
        String emptyOrdersUri = testRootUri + "/empty_orders";
        buildOrders(ordersUri);
        buildEmptyOrders(emptyOrdersUri);

        TableCatalog.atDefaultLocation().register("orders", ordersUri);
        TableCatalog.atDefaultLocation().register("empty_orders", emptyOrdersUri);

        QueryCommand.run(new String[] {"--sql",
                "SELECT o.id FROM orders o WHERE o.id != (SELECT COUNT(e.amount) AS c FROM empty_orders e)"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("3 row(s)"),
                "COUNT of zero rows is 0, not NULL -- every id (1,2,3) != 0, from real MinIO: " + output);
    }
}
