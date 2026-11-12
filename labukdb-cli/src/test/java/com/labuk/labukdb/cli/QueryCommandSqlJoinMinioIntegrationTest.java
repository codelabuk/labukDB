package com.labuk.labukdb.cli;

import com.labuk.labukdb.core.catalog.TableCatalog;
import com.labuk.labukdb.core.encode.ArrowToParquetWriter;
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
 * Closes the one gap the SQL engine's LEFT/RIGHT/FULL/3+-way join work left
 * open: {@link QueryCommandSqlJoinIntegrationTest} proves correctness
 * against local fixtures only. This is the first test that runs an actual
 * {@code --sql} LEFT/FULL/3-way join against a real MinIO cluster, not an
 * inferred-safe composition of "joins work locally" + "single-table reads
 * work against MinIO" — same opt-in {@code MINIO_*} env-var gate as every
 * other MinIO integration test in this project.
 */
class QueryCommandSqlJoinMinioIntegrationTest {

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
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MINIO_ENDPOINT not set -- skipping live MinIO join test");
        assumeTrue(accessKey != null && !accessKey.isBlank(), "MINIO_ACCESS_KEY not set -- skipping live MinIO join test");
        assumeTrue(secretKey != null && !secretKey.isBlank(), "MINIO_SECRET_KEY not set -- skipping live MinIO join test");

        String bucket = System.getenv("MINIO_TEST_BUCKET");
        if (bucket == null || bucket.isBlank()) {
            bucket = "warehouse";
        }

        allocator = new RootAllocator(Long.MAX_VALUE);
        conf = MinioConfiguration.forMinio(endpoint, accessKey, secretKey);
        testRootUri = "s3a://" + bucket + "/labukdb-cli-join-minio-test/" + UUID.randomUUID();
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

    /** customers (Delta, remote): id(long), region(string). id 1,2,3 (3 has no matching order -- the FULL JOIN's unmatched-right case). */
    private void buildCustomersWithExtra(String uri) throws Exception {
        Field id = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field region = new Field("region", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Schema schema = new Schema(List.of(id, region));
        TableLocation location = TableLocation.forRemote(uri, conf);
        new TableCreator().createTable(location, schema, List.of(), null);

        try (VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
            batch.allocateNew();
            BigIntVector idVector = (BigIntVector) batch.getVector("id");
            VarCharVector regionVector = (VarCharVector) batch.getVector("region");
            long[] ids = {1, 2, 3};
            String[] regions = {"us-east", "eu-west", "fr-south"};
            for (int i = 0; i < ids.length; i++) {
                idVector.setSafe(i, ids[i]);
                regionVector.setSafe(i, regions[i].getBytes(StandardCharsets.UTF_8));
            }
            batch.setRowCount(ids.length);
            CommitWriter.forLocation(location).appendBatch(batch, Map.of());
        }
    }

    /** customers (Delta, remote), plain: id(long), region(string). id 1,2 -- no extra unmatched row, for the 3-way test. */
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

    private static final org.apache.iceberg.Schema ICEBERG_REGIONS_SCHEMA = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "name", Types.StringType.get()),
            Types.NestedField.required(2, "manager", Types.StringType.get()));

    /** regions (Iceberg, remote, unpartitioned): name(string), manager(string) -- keyed by the SAME region names buildCustomers produces. */
    private void buildRegions(String uri) throws Exception {
        HadoopTables tables = new HadoopTables(conf);
        Table table = tables.create(ICEBERG_REGIONS_SCHEMA, PartitionSpec.unpartitioned(), SortOrder.unsorted(), Map.of(), uri);

        Field name = new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field manager = new Field("manager", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        try (VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(name, manager)), allocator)) {
            batch.allocateNew();
            VarCharVector nameVector = (VarCharVector) batch.getVector("name");
            VarCharVector managerVector = (VarCharVector) batch.getVector("manager");
            String[] names = {"us-east", "eu-west"};
            String[] managers = {"Alice", "Bob"};
            for (int i = 0; i < names.length; i++) {
                nameVector.setSafe(i, names[i].getBytes(StandardCharsets.UTF_8));
                managerVector.setSafe(i, managers[i].getBytes(StandardCharsets.UTF_8));
            }
            batch.setRowCount(names.length);

            String remoteDataFileUri = uri + "/part-0.parquet";
            TableLocation remoteDataFile = TableLocation.forRemote(remoteDataFileUri, conf);
            new ArrowToParquetWriter().write(remoteDataFile, batch);

            long sizeBytes = fileSystem.getFileStatus(new Path(remoteDataFileUri)).getLen();
            DataFile dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath(remoteDataFileUri)
                    .withFileSizeInBytes(sizeBytes)
                    .withRecordCount(names.length)
                    .build();
            table.newAppend().appendFile(dataFile).commit();
        }
    }

    @Test
    void leftJoin_realMinioBackedDeltaTables_preservesUnmatchedLeftRowWithNulls() throws Exception {
        String ordersUri = testRootUri + "/orders";
        String customersUri = testRootUri + "/customers";
        buildOrders(ordersUri);
        buildCustomersWithExtra(customersUri);

        TableCatalog.atDefaultLocation().register("orders", ordersUri);
        TableCatalog.atDefaultLocation().register("customers", customersUri);

        QueryCommand.run(new String[] {"--sql",
                "SELECT a.service, b.region FROM orders a LEFT JOIN customers b ON a.customer_id = b.id"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("auth-svc | us-east"), output);
        assertTrue(output.contains("payments-svc | eu-west"), output);
        assertTrue(output.contains("orphan-svc | NULL"), "LEFT JOIN must keep the unmatched left row, null-padded, from real MinIO: " + output);
        assertTrue(output.contains("3 row(s)"), output);
    }

    @Test
    void fullJoin_realMinioBackedDeltaTables_preservesUnmatchedRowsOnBothSides() throws Exception {
        String ordersUri = testRootUri + "/orders";
        String customersUri = testRootUri + "/customers";
        buildOrders(ordersUri); // customer_id 1, 2, 999 (999 unmatched)
        buildCustomersWithExtra(customersUri); // id 1, 2, 3 (3 unmatched)

        TableCatalog.atDefaultLocation().register("orders", ordersUri);
        TableCatalog.atDefaultLocation().register("customers", customersUri);

        QueryCommand.run(new String[] {"--sql",
                "SELECT a.service, b.region FROM orders a FULL JOIN customers b ON a.customer_id = b.id"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("auth-svc | us-east"), output);
        assertTrue(output.contains("payments-svc | eu-west"), output);
        assertTrue(output.contains("orphan-svc | NULL"), "unmatched left (customer_id=999) must be preserved, null-padded, from real MinIO: " + output);
        assertTrue(output.contains("NULL | fr-south"), "unmatched right (id=3) must be preserved, null-padded, from real MinIO: " + output);
        assertTrue(output.contains("4 row(s)"), output);
    }

    /**
     * A real 3-way left-deep chain against real MinIO, Delta -> Delta ->
     * Iceberg, where the second step's ON condition keys against
     * {@code b.region} -- a column introduced by the FIRST step, not the
     * base table -- exercising the exact same already-alias-qualified
     * key-naming path {@code QueryCommandSqlJoinIntegrationTest}'s local
     * version does, now against real remote files for every table.
     */
    @Test
    void threeWayJoin_realMinioBackedDeltaToDeltaToIceberg_keysTheSecondStepOffTheFirstStepsTable() throws Exception {
        String ordersUri = testRootUri + "/orders";
        String customersUri = testRootUri + "/customers";
        String regionsUri = testRootUri + "/regions";
        buildOrders(ordersUri); // customer_id 1, 2, 999 (999 unmatched -> dropped by the first INNER step)
        buildCustomers(customersUri);
        buildRegions(regionsUri);

        TableCatalog.atDefaultLocation().register("orders", ordersUri);
        TableCatalog.atDefaultLocation().register("customers", customersUri);
        TableCatalog.atDefaultLocation().register("regions", regionsUri);

        QueryCommand.run(new String[] {"--sql",
                "SELECT a.service, b.region, c.manager FROM orders a "
                        + "JOIN customers b ON a.customer_id = b.id "
                        + "JOIN regions c ON b.region = c.name"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("auth-svc | us-east | Alice"), output);
        assertTrue(output.contains("payments-svc | eu-west | Bob"), output);
        assertFalse(output.contains("orphan-svc"), output);
        assertTrue(output.contains("2 row(s)"), output);
    }
}
