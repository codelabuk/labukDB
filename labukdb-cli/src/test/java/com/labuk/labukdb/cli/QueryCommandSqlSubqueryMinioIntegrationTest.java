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
 * Closes the one gap the SQL engine Phase 4 increment 1 work left open:
 * {@link QueryCommandSqlSubqueryIntegrationTest} proves correlated/
 * uncorrelated {@code EXISTS}/{@code IN (SELECT...)} against local fixtures
 * only. This is the first test that runs an actual decorrelated subquery
 * against a real MinIO cluster -- same opt-in {@code MINIO_*} env-var gate
 * as every other MinIO integration test in this project.
 */
class QueryCommandSqlSubqueryMinioIntegrationTest {

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
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MINIO_ENDPOINT not set -- skipping live MinIO subquery test");
        assumeTrue(accessKey != null && !accessKey.isBlank(), "MINIO_ACCESS_KEY not set -- skipping live MinIO subquery test");
        assumeTrue(secretKey != null && !secretKey.isBlank(), "MINIO_SECRET_KEY not set -- skipping live MinIO subquery test");

        String bucket = System.getenv("MINIO_TEST_BUCKET");
        if (bucket == null || bucket.isBlank()) {
            bucket = "warehouse";
        }

        allocator = new RootAllocator(Long.MAX_VALUE);
        conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
        testRootUri = "s3a://" + bucket + "/labukdb-cli-subquery-minio-test/" + UUID.randomUUID();
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

    /** orders (Delta, remote): customer_id(long), service(string). customer_id 1,2,999 (999 has no matching customer). */
    private void buildOrders(String uri) throws Exception {
        Field customerId = new Field("customer_id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field service = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Schema schema = new Schema(List.of(customerId, service));
        TableLocation location = TableLocation.forRemote(uri, conf);
        new TableCreator().createTable(location, schema, List.of(), null);

        try (VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
            batch.allocateNew();
            BigIntVector idVector = (BigIntVector) batch.getVector("customer_id");
            VarCharVector serviceVector = (VarCharVector) batch.getVector("service");
            long[] ids = {1, 2, 999};
            String[] services = {"auth-svc", "payments-svc", "orphan-svc"};
            for (int i = 0; i < ids.length; i++) {
                idVector.setSafe(i, ids[i]);
                serviceVector.setSafe(i, services[i].getBytes(StandardCharsets.UTF_8));
            }
            batch.setRowCount(ids.length);
            CommitWriter.forLocation(location).appendBatch(batch, Map.of());
        }
    }

    /** customers (Delta, remote): id(long), region(string). id 1,2. */
    private void buildCustomers(String uri) throws Exception {
        Field id = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field region = new Field("region", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Schema schema = new Schema(List.of(id, region));
        TableLocation location = TableLocation.forRemote(uri, conf);
        new TableCreator().createTable(location, schema, List.of(), null);

        try (VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
            batch.allocateNew();
            BigIntVector idVector = (BigIntVector) batch.getVector("id");
            VarCharVector regionVector = (VarCharVector) batch.getVector("region");
            long[] ids = {1, 2};
            String[] regions = {"us-east", "eu-west"};
            for (int i = 0; i < ids.length; i++) {
                idVector.setSafe(i, ids[i]);
                regionVector.setSafe(i, regions[i].getBytes(StandardCharsets.UTF_8));
            }
            batch.setRowCount(ids.length);
            CommitWriter.forLocation(location).appendBatch(batch, Map.of());
        }
    }

    @Test
    void correlatedExists_realMinioBackedDeltaTables_matchesCorrectlyAndDropsTheUnmatchedRow() throws Exception {
        String ordersUri = testRootUri + "/orders";
        String customersUri = testRootUri + "/customers";
        buildOrders(ordersUri);
        buildCustomers(customersUri);

        TableCatalog.atDefaultLocation().register("orders", ordersUri);
        TableCatalog.atDefaultLocation().register("customers", customersUri);

        QueryCommand.run(new String[] {"--sql",
                "SELECT a.service FROM orders a WHERE EXISTS "
                        + "(SELECT 1 FROM customers b WHERE b.id = a.customer_id)"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("auth-svc"), output);
        assertTrue(output.contains("payments-svc"), output);
        assertFalse(output.contains("orphan-svc"), "customer_id=999 has no matching customer -- EXISTS must drop it, from real MinIO: " + output);
        assertTrue(output.contains("2 row(s)"), output);
    }

    @Test
    void uncorrelatedInSubquery_realMinioBackedDeltaTables_matchesCorrectlyAndDropsTheUnmatchedRow() throws Exception {
        String ordersUri = testRootUri + "/orders";
        String customersUri = testRootUri + "/customers";
        buildOrders(ordersUri);
        buildCustomers(customersUri);

        TableCatalog.atDefaultLocation().register("orders", ordersUri);
        TableCatalog.atDefaultLocation().register("customers", customersUri);

        QueryCommand.run(new String[] {"--sql",
                "SELECT a.service FROM orders a WHERE a.customer_id IN (SELECT b.id FROM customers b)"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("auth-svc"), output);
        assertTrue(output.contains("payments-svc"), output);
        assertFalse(output.contains("orphan-svc"), output);
        assertTrue(output.contains("2 row(s)"), output);
    }

    @Test
    void correlatedNotExists_realMinioBackedDeltaTables_keepsOnlyTheUnmatchedRow() throws Exception {
        String ordersUri = testRootUri + "/orders";
        String customersUri = testRootUri + "/customers";
        buildOrders(ordersUri);
        buildCustomers(customersUri);

        TableCatalog.atDefaultLocation().register("orders", ordersUri);
        TableCatalog.atDefaultLocation().register("customers", customersUri);

        QueryCommand.run(new String[] {"--sql",
                "SELECT a.service FROM orders a WHERE NOT EXISTS "
                        + "(SELECT 1 FROM customers b WHERE b.id = a.customer_id)"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("orphan-svc"), output);
        assertFalse(output.contains("auth-svc"), output);
        assertFalse(output.contains("payments-svc"), output);
        assertTrue(output.contains("1 row(s)"), output);
    }
}
