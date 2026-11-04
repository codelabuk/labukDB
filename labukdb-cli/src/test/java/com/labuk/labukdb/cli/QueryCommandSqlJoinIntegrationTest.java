package com.labuk.labukdb.cli;

import com.labuk.labukdb.core.catalog.TableCatalog;
import com.labuk.labukdb.core.encode.ArrowToParquetWriter;
import com.labuk.labukdb.core.log.CommitWriter;
import com.labuk.labukdb.core.log.TableCreator;
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
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SQL engine Phase 3, increment 1: a real two-table {@code INNER} equi-join
 * through {@code --sql}, end to end. One test deliberately joins a Delta
 * table with an Iceberg table — proving (not just asserting in prose)
 * that each side resolving completely independently is a genuine, free
 * consequence of the design, not a specifically-built feature.
 */
class QueryCommandSqlJoinIntegrationTest {

    private String originalUserHome;
    private PrintStream originalOut;
    private ByteArrayOutputStream capturedOut;

    @BeforeEach
    void setUp(@TempDir Path fakeHome) {
        originalUserHome = System.getProperty("user.home");
        System.setProperty("user.home", fakeHome.toString());
        originalOut = System.out;
        capturedOut = new ByteArrayOutputStream();
        System.setOut(new PrintStream(capturedOut, true, StandardCharsets.UTF_8));
    }

    @AfterEach
    void tearDown() {
        System.setOut(originalOut);
        System.setProperty("user.home", originalUserHome);
    }

    /** orders: customer_id(long), service(string). customer_id 1,2,999 (999 has no matching customer). */
    private void buildDeltaOrders(Path tableRoot) throws Exception {
        Field customerId = new Field("customer_id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field service = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Schema schema = new Schema(List.of(customerId, service));
        TableLocation location = TableLocation.forLocal(tableRoot);
        new TableCreator().createTable(location, schema, List.of(), null);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
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

    /** customers (Delta): id(long), region(string). id 1,2. */
    private void buildDeltaCustomers(Path tableRoot) throws Exception {
        Field id = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field region = new Field("region", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Schema schema = new Schema(List.of(id, region));
        TableLocation location = TableLocation.forLocal(tableRoot);
        new TableCreator().createTable(location, schema, List.of(), null);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
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

    private static final org.apache.iceberg.Schema ICEBERG_CUSTOMERS_SCHEMA = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "region", Types.StringType.get()));

    /** customers (Iceberg, unpartitioned): id(long), region(string). id 1,2. */
    private void buildIcebergCustomers(Path tableRoot) throws Exception {
        Table table = new HadoopTables(new Configuration())
                .create(ICEBERG_CUSTOMERS_SCHEMA, PartitionSpec.unpartitioned(), SortOrder.unsorted(), Map.of(),
                        tableRoot.toString());

        Field id = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field region = new Field("region", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(id, region)), allocator)) {
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

            Path dataFile = tableRoot.resolve("part-0.parquet");
            new ArrowToParquetWriter().write(dataFile.toFile(), batch);

            DataFile icebergFile = DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath(dataFile.toAbsolutePath().toString())
                    .withFileSizeInBytes(dataFile.toFile().length())
                    .withRecordCount(ids.length)
                    .build();
            table.newAppend().appendFile(icebergFile).commit();
        }
    }

    @Test
    void innerJoin_deltaToDelta_matchesCorrectlyAndDropsOrphan(@TempDir Path root) throws Exception {
        Path ordersRoot = root.resolve("orders");
        Path customersRoot = root.resolve("customers");
        buildDeltaOrders(ordersRoot);
        buildDeltaCustomers(customersRoot);

        TableCatalog.atDefaultLocation().register("orders", ordersRoot.toAbsolutePath().toString());
        TableCatalog.atDefaultLocation().register("customers", customersRoot.toAbsolutePath().toString());

        QueryCommand.run(new String[] {"--sql",
                "SELECT a.service, b.region FROM orders a JOIN customers b ON a.customer_id = b.id"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("a.service | b.region"), output);
        assertTrue(output.contains("auth-svc | us-east"), output);
        assertTrue(output.contains("payments-svc | eu-west"), output);
        assertFalse(output.contains("orphan-svc"), "customer_id=999 has no matching customer -- INNER JOIN must drop it: " + output);
        assertTrue(output.contains("2 row(s)"), output);
    }

    /**
     * Proves the "free consequence" design claim for real: each side
     * resolves through its own independent format-detection branch in
     * {@code QueryCommand}, so a Delta table and an Iceberg table join
     * correctly with zero format-specific join code.
     */
    @Test
    void innerJoin_deltaToIceberg_resolvesEachSideIndependently(@TempDir Path root) throws Exception {
        Path ordersRoot = root.resolve("orders"); // Delta
        Path customersRoot = root.resolve("customers"); // Iceberg
        buildDeltaOrders(ordersRoot);
        buildIcebergCustomers(customersRoot);

        TableCatalog.atDefaultLocation().register("orders", ordersRoot.toAbsolutePath().toString());
        TableCatalog.atDefaultLocation().register("customers", customersRoot.toAbsolutePath().toString());

        QueryCommand.run(new String[] {"--sql",
                "SELECT a.service, b.region FROM orders a JOIN customers b ON a.customer_id = b.id"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("auth-svc | us-east"), output);
        assertTrue(output.contains("payments-svc | eu-west"), output);
        assertTrue(output.contains("2 row(s)"), output);
    }

    /** orders variant with an amount column, customer_id 1 appearing twice -- real multi-row aggregation per group, not a 1-row-per-group coincidence. */
    private void buildDeltaOrdersWithAmount(Path tableRoot) throws Exception {
        Field customerId = new Field("customer_id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field service = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field amount = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Schema schema = new Schema(List.of(customerId, service, amount));
        TableLocation location = TableLocation.forLocal(tableRoot);
        new TableCreator().createTable(location, schema, List.of(), null);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
            batch.allocateNew();
            BigIntVector idVector = (BigIntVector) batch.getVector("customer_id");
            VarCharVector serviceVector = (VarCharVector) batch.getVector("service");
            BigIntVector amountVector = (BigIntVector) batch.getVector("amount");
            long[] ids = {1, 1, 2};
            String[] services = {"auth-svc", "gateway-svc", "payments-svc"};
            long[] amounts = {10, 30, 20};
            for (int i = 0; i < ids.length; i++) {
                idVector.setSafe(i, ids[i]);
                serviceVector.setSafe(i, services[i].getBytes(StandardCharsets.UTF_8));
                amountVector.setSafe(i, amounts[i]);
            }
            batch.setRowCount(ids.length);
            CommitWriter.forLocation(location).appendBatch(batch, Map.of());
        }
    }

    /** SQL engine Phase 3, increment 2: JOIN + GROUP BY + aggregate, end to end. */
    @Test
    void innerJoin_withGroupByAndAggregate_combinesAcrossMultipleRowsPerGroup(@TempDir Path root) throws Exception {
        Path ordersRoot = root.resolve("orders");
        Path customersRoot = root.resolve("customers");
        buildDeltaOrdersWithAmount(ordersRoot);
        buildDeltaCustomers(customersRoot);

        TableCatalog.atDefaultLocation().register("orders", ordersRoot.toAbsolutePath().toString());
        TableCatalog.atDefaultLocation().register("customers", customersRoot.toAbsolutePath().toString());

        QueryCommand.run(new String[] {"--sql",
                "SELECT b.region, SUM(a.amount) AS total FROM orders a JOIN customers b "
                        + "ON a.customer_id = b.id GROUP BY b.region"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("us-east | 40"), "customer_id=1's two orders (10+30) must combine into one us-east group: " + output);
        assertTrue(output.contains("eu-west | 20"), output);
        assertTrue(output.contains("2 row(s)"), output);
    }

    /** SQL engine Phase 3, increment 2: JOIN + ORDER BY + LIMIT (no GROUP BY), end to end. */
    @Test
    void innerJoin_withOrderByAndLimit_sortsAndCapsTheCombinedOutput(@TempDir Path root) throws Exception {
        Path ordersRoot = root.resolve("orders");
        Path customersRoot = root.resolve("customers");
        buildDeltaOrdersWithAmount(ordersRoot);
        buildDeltaCustomers(customersRoot);

        TableCatalog.atDefaultLocation().register("orders", ordersRoot.toAbsolutePath().toString());
        TableCatalog.atDefaultLocation().register("customers", customersRoot.toAbsolutePath().toString());

        QueryCommand.run(new String[] {"--sql",
                "SELECT a.service, b.region FROM orders a JOIN customers b "
                        + "ON a.customer_id = b.id ORDER BY a.amount DESC LIMIT 2"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("2 row(s)"), output);
        assertFalse(output.contains("auth-svc"), "amount=10 (lowest) must be cut by LIMIT 2: " + output);
        int gatewayIndex = output.indexOf("gateway-svc"); // amount=30, highest
        int paymentsIndex = output.indexOf("payments-svc"); // amount=20
        assertTrue(gatewayIndex >= 0 && paymentsIndex >= 0, output);
        assertTrue(gatewayIndex < paymentsIndex, "ORDER BY amount DESC must put gateway-svc (30) before payments-svc (20): " + output);
    }
}
