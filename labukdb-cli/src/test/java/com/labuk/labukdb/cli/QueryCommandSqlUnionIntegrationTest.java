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
 * SQL engine: {@code UNION}/{@code UNION ALL} through {@code --sql}, end to
 * end. Each branch is translated and resolved completely independently
 * (see {@code SqlQueryTranslator}/{@code QueryCommand}'s own javadoc) --
 * these tests prove that holds for real, including a branch that's itself
 * a join, and a union spanning a Delta table and an Iceberg table.
 */
class QueryCommandSqlUnionIntegrationTest {

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

    /** service(string), amount(long) -- a plain two-column Delta table, one commit per call. */
    private void buildDeltaTable(Path tableRoot, String[] services, long[] amounts) throws Exception {
        Field service = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field amount = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Schema schema = new Schema(List.of(service, amount));
        TableLocation location = TableLocation.forLocal(tableRoot);
        new TableCreator().createTable(location, schema, List.of(), null);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
            batch.allocateNew();
            VarCharVector serviceVector = (VarCharVector) batch.getVector("service");
            BigIntVector amountVector = (BigIntVector) batch.getVector("amount");
            for (int i = 0; i < services.length; i++) {
                serviceVector.setSafe(i, services[i].getBytes(StandardCharsets.UTF_8));
                amountVector.setSafe(i, amounts[i]);
            }
            batch.setRowCount(services.length);
            CommitWriter.forLocation(location).appendBatch(batch, Map.of());
        }
    }

    private static final org.apache.iceberg.Schema ICEBERG_SCHEMA = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "service", Types.StringType.get()),
            Types.NestedField.required(2, "amount", Types.LongType.get()));

    /** Same (service, amount) shape as {@link #buildDeltaTable}, but as an Iceberg table. */
    private void buildIcebergTable(Path tableRoot, String[] services, long[] amounts) throws Exception {
        Table table = new HadoopTables(new Configuration())
                .create(ICEBERG_SCHEMA, PartitionSpec.unpartitioned(), SortOrder.unsorted(), Map.of(),
                        tableRoot.toString());

        Field service = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field amount = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(service, amount)), allocator)) {
            batch.allocateNew();
            VarCharVector serviceVector = (VarCharVector) batch.getVector("service");
            BigIntVector amountVector = (BigIntVector) batch.getVector("amount");
            for (int i = 0; i < services.length; i++) {
                serviceVector.setSafe(i, services[i].getBytes(StandardCharsets.UTF_8));
                amountVector.setSafe(i, amounts[i]);
            }
            batch.setRowCount(services.length);

            Path dataFile = tableRoot.resolve("part-0.parquet");
            new ArrowToParquetWriter().write(dataFile.toFile(), batch);

            DataFile icebergFile = DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath(dataFile.toAbsolutePath().toString())
                    .withFileSizeInBytes(dataFile.toFile().length())
                    .withRecordCount(services.length)
                    .build();
            table.newAppend().appendFile(icebergFile).commit();
        }
    }

    @Test
    void unionAll_twoDeltaTables_keepsEveryRowFromBothSides(@TempDir Path root) throws Exception {
        Path errorsRoot = root.resolve("errors");
        Path infosRoot = root.resolve("infos");
        buildDeltaTable(errorsRoot, new String[] {"auth-svc"}, new long[] {1});
        buildDeltaTable(infosRoot, new String[] {"gateway-svc", "auth-svc"}, new long[] {2, 1});

        TableCatalog.atDefaultLocation().register("errors", errorsRoot.toAbsolutePath().toString());
        TableCatalog.atDefaultLocation().register("infos", infosRoot.toAbsolutePath().toString());

        QueryCommand.run(new String[] {"--sql",
                "SELECT service, amount FROM errors UNION ALL SELECT service, amount FROM infos"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("auth-svc | 1"), output);
        assertTrue(output.contains("gateway-svc | 2"), output);
        // "auth-svc | 1" genuinely appears twice -- UNION ALL keeps the duplicate.
        assertEquals(2, output.split("auth-svc \\| 1", -1).length - 1, output);
        assertTrue(output.contains("3 row(s)"), output);
    }

    @Test
    void plainUnion_duplicateRowAcrossBranches_removedOnce(@TempDir Path root) throws Exception {
        Path errorsRoot = root.resolve("errors");
        Path infosRoot = root.resolve("infos");
        buildDeltaTable(errorsRoot, new String[] {"auth-svc"}, new long[] {1});
        buildDeltaTable(infosRoot, new String[] {"gateway-svc", "auth-svc"}, new long[] {2, 1});

        TableCatalog.atDefaultLocation().register("errors", errorsRoot.toAbsolutePath().toString());
        TableCatalog.atDefaultLocation().register("infos", infosRoot.toAbsolutePath().toString());

        QueryCommand.run(new String[] {"--sql",
                "SELECT service, amount FROM errors UNION SELECT service, amount FROM infos"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertEquals(1, output.split("auth-svc \\| 1", -1).length - 1,
                "plain UNION must remove the duplicate row: " + output);
        assertTrue(output.contains("gateway-svc | 2"), output);
        assertTrue(output.contains("2 row(s)"), output);
    }

    @Test
    void oneBranchEmpty_resultIsJustTheOtherBranch(@TempDir Path root) throws Exception {
        Path errorsRoot = root.resolve("errors");
        Path infosRoot = root.resolve("infos");
        buildDeltaTable(errorsRoot, new String[] {"auth-svc"}, new long[] {1});
        buildDeltaTable(infosRoot, new String[] {"gateway-svc"}, new long[] {2});

        TableCatalog.atDefaultLocation().register("errors", errorsRoot.toAbsolutePath().toString());
        TableCatalog.atDefaultLocation().register("infos", infosRoot.toAbsolutePath().toString());

        QueryCommand.run(new String[] {"--sql",
                "SELECT service, amount FROM errors WHERE amount > 100 "
                        + "UNION ALL SELECT service, amount FROM infos"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertFalse(output.contains("auth-svc"), output);
        assertTrue(output.contains("gateway-svc | 2"), output);
        assertTrue(output.contains("1 row(s)"), output);
    }

    @Test
    void bothBranchesEmpty_printsZeroRowsWithNoHeader(@TempDir Path root) throws Exception {
        Path errorsRoot = root.resolve("errors");
        Path infosRoot = root.resolve("infos");
        buildDeltaTable(errorsRoot, new String[] {"auth-svc"}, new long[] {1});
        buildDeltaTable(infosRoot, new String[] {"gateway-svc"}, new long[] {2});

        TableCatalog.atDefaultLocation().register("errors", errorsRoot.toAbsolutePath().toString());
        TableCatalog.atDefaultLocation().register("infos", infosRoot.toAbsolutePath().toString());

        QueryCommand.run(new String[] {"--sql",
                "SELECT service, amount FROM errors WHERE amount > 100 "
                        + "UNION ALL SELECT service, amount FROM infos WHERE amount > 100"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertEquals("0 row(s)", output.trim(), output);
    }

    @Test
    void columnCountMismatchAcrossBranches_rejectedWithClearError(@TempDir Path root) throws Exception {
        Path errorsRoot = root.resolve("errors");
        Path infosRoot = root.resolve("infos");
        buildDeltaTable(errorsRoot, new String[] {"auth-svc"}, new long[] {1});
        buildDeltaTable(infosRoot, new String[] {"gateway-svc"}, new long[] {2});

        TableCatalog.atDefaultLocation().register("errors", errorsRoot.toAbsolutePath().toString());
        TableCatalog.atDefaultLocation().register("infos", infosRoot.toAbsolutePath().toString());

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () ->
                QueryCommand.run(new String[] {"--sql",
                        "SELECT service FROM errors UNION ALL SELECT service, amount FROM infos"}));
        assertTrue(thrown.getMessage().contains("same number of"), thrown.getMessage());
    }

    @Test
    void unionAll_deltaAndIceberg_resolvesEachSideIndependently(@TempDir Path root) throws Exception {
        Path errorsRoot = root.resolve("errors"); // Delta
        Path infosRoot = root.resolve("infos"); // Iceberg
        buildDeltaTable(errorsRoot, new String[] {"auth-svc"}, new long[] {1});
        buildIcebergTable(infosRoot, new String[] {"gateway-svc"}, new long[] {2});

        TableCatalog.atDefaultLocation().register("errors", errorsRoot.toAbsolutePath().toString());
        TableCatalog.atDefaultLocation().register("infos", infosRoot.toAbsolutePath().toString());

        QueryCommand.run(new String[] {"--sql",
                "SELECT service, amount FROM errors UNION ALL SELECT service, amount FROM infos"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("auth-svc | 1"), output);
        assertTrue(output.contains("gateway-svc | 2"), output);
        assertTrue(output.contains("2 row(s)"), output);
    }

    /** customers: id(long), region(string). id 1,2. */
    private void buildCustomers(Path tableRoot) throws Exception {
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

    /** orders: id(long), customer_id(long), service(string). id 1 -> customer 1, id 2 -> customer 2. */
    private void buildOrders(Path tableRoot) throws Exception {
        Field id = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field customerId = new Field("customer_id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field service = new Field("service", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Schema schema = new Schema(List.of(id, customerId, service));
        TableLocation location = TableLocation.forLocal(tableRoot);
        new TableCreator().createTable(location, schema, List.of(), null);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
            batch.allocateNew();
            BigIntVector idVector = (BigIntVector) batch.getVector("id");
            BigIntVector customerIdVector = (BigIntVector) batch.getVector("customer_id");
            VarCharVector serviceVector = (VarCharVector) batch.getVector("service");
            long[] ids = {1, 2};
            long[] customerIds = {1, 2};
            String[] services = {"auth-svc", "payments-svc"};
            for (int i = 0; i < ids.length; i++) {
                idVector.setSafe(i, ids[i]);
                customerIdVector.setSafe(i, customerIds[i]);
                serviceVector.setSafe(i, services[i].getBytes(StandardCharsets.UTF_8));
            }
            batch.setRowCount(ids.length);
            CommitWriter.forLocation(location).appendBatch(batch, Map.of());
        }
    }

    /**
     * A branch that's itself a two-table JOIN, unioned with a plain
     * single-table branch -- proves the branch really is an ordinary,
     * fully-capable {@code SqlQuery}, not a narrowed-down shape.
     */
    @Test
    void unionAll_oneBranchIsAJoin_resolvesCorrectly(@TempDir Path root) throws Exception {
        Path ordersRoot = root.resolve("orders");
        Path customersRoot = root.resolve("customers");
        Path infosRoot = root.resolve("infos");
        buildOrders(ordersRoot);
        buildCustomers(customersRoot);
        buildDeltaTable(infosRoot, new String[] {"standalone-svc"}, new long[] {99});

        TableCatalog.atDefaultLocation().register("orders", ordersRoot.toAbsolutePath().toString());
        TableCatalog.atDefaultLocation().register("customers", customersRoot.toAbsolutePath().toString());
        TableCatalog.atDefaultLocation().register("infos", infosRoot.toAbsolutePath().toString());

        QueryCommand.run(new String[] {"--sql",
                "SELECT a.service, b.id FROM orders a JOIN customers b ON a.customer_id = b.id "
                        + "UNION ALL "
                        + "SELECT service, amount FROM infos"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("auth-svc | 1"), output);
        assertTrue(output.contains("payments-svc | 2"), output);
        assertTrue(output.contains("standalone-svc | 99"), output);
        assertTrue(output.contains("3 row(s)"), output);
    }
}
