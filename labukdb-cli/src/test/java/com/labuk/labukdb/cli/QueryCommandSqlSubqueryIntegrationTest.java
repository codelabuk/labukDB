package com.labuk.labukdb.cli;

import com.labuk.labukdb.core.catalog.TableCatalog;
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
 * SQL engine Phase 4, increment 1: {@code IN}/{@code NOT IN}/{@code EXISTS}/
 * {@code NOT EXISTS} subqueries, end to end through {@code --sql}, each
 * decorrelated by {@code SqlQueryTranslator} into a {@code JoinType.SEMI}/
 * {@code ANTI} step and executed by the exact same left-deep join-chain
 * machinery {@link QueryCommandSqlJoinIntegrationTest} already proves for
 * real {@code JOIN}s -- no new execution path, same as the design intent.
 */
class QueryCommandSqlSubqueryIntegrationTest {

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
    private void buildOrders(Path tableRoot) throws Exception {
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

    /** customers_with_null: id(long), region(string). id 1, then a row with a NULL id. */
    private void buildCustomersWithNullId(Path tableRoot) throws Exception {
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
            idVector.setSafe(0, 1L);
            regionVector.setSafe(0, "us-east".getBytes(StandardCharsets.UTF_8));
            idVector.setNull(1);
            regionVector.setSafe(1, "unknown".getBytes(StandardCharsets.UTF_8));
            batch.setRowCount(2);
            CommitWriter.forLocation(location).appendBatch(batch, Map.of());
        }
    }

    private void registerTables(Path root) throws Exception {
        Path ordersRoot = root.resolve("orders");
        Path customersRoot = root.resolve("customers");
        buildOrders(ordersRoot);
        buildCustomers(customersRoot);
        buildCustomersWithNullId(root.resolve("customers_with_null"));
        TableCatalog.atDefaultLocation().register("orders", ordersRoot.toAbsolutePath().toString());
        TableCatalog.atDefaultLocation().register("customers", customersRoot.toAbsolutePath().toString());
        TableCatalog.atDefaultLocation().register("customers_with_null",
                root.resolve("customers_with_null").toAbsolutePath().toString());
    }

    @Test
    void correlatedExists_matchesCorrectlyAndDropsTheUnmatchedRow(@TempDir Path root) throws Exception {
        registerTables(root);

        QueryCommand.run(new String[] {"--sql",
                "SELECT a.service FROM orders a WHERE EXISTS "
                        + "(SELECT 1 FROM customers b WHERE b.id = a.customer_id)"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("auth-svc"), output);
        assertTrue(output.contains("payments-svc"), output);
        assertFalse(output.contains("orphan-svc"), "customer_id=999 has no matching customer -- EXISTS must drop it: " + output);
        assertTrue(output.contains("2 row(s)"), output);
    }

    @Test
    void correlatedNotExists_keepsOnlyTheUnmatchedRow(@TempDir Path root) throws Exception {
        registerTables(root);

        QueryCommand.run(new String[] {"--sql",
                "SELECT a.service FROM orders a WHERE NOT EXISTS "
                        + "(SELECT 1 FROM customers b WHERE b.id = a.customer_id)"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("orphan-svc"), output);
        assertFalse(output.contains("auth-svc"), output);
        assertFalse(output.contains("payments-svc"), output);
        assertTrue(output.contains("1 row(s)"), output);
    }

    @Test
    void uncorrelatedInSubquery_matchesCorrectlyAndDropsTheUnmatchedRow(@TempDir Path root) throws Exception {
        registerTables(root);

        QueryCommand.run(new String[] {"--sql",
                "SELECT a.service FROM orders a WHERE a.customer_id IN (SELECT b.id FROM customers b)"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("auth-svc"), output);
        assertTrue(output.contains("payments-svc"), output);
        assertFalse(output.contains("orphan-svc"), output);
        assertTrue(output.contains("2 row(s)"), output);
    }

    @Test
    void notInSubquery_keepsOnlyTheUnmatchedRow(@TempDir Path root) throws Exception {
        registerTables(root);

        QueryCommand.run(new String[] {"--sql",
                "SELECT a.service FROM orders a WHERE a.customer_id NOT IN (SELECT b.id FROM customers b)"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("orphan-svc"), output);
        assertFalse(output.contains("auth-svc"), output);
        assertFalse(output.contains("payments-svc"), output);
        assertTrue(output.contains("1 row(s)"), output);
    }

    /**
     * Named gap, now fixed: a scalar subquery nested inside an {@code IN
     * (SELECT ...)} subquery's own filter. {@code customers} has ids
     * {1,2}, so {@code MAX(c.id)} is 2 -- only {@code b.id = 2} survives
     * the subquery's own filter, so the {@code IN} set is just {2}, and
     * only {@code customer_id = 2}'s row ("payments-svc") matches.
     */
    @Test
    void inSubquery_withAScalarSubqueryInsideItsOwnFilter_resolvesCorrectly(@TempDir Path root) throws Exception {
        registerTables(root);

        QueryCommand.run(new String[] {"--sql",
                "SELECT a.service FROM orders a WHERE a.customer_id IN "
                        + "(SELECT b.id FROM customers b WHERE b.id = (SELECT MAX(c.id) AS m FROM customers c))"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("payments-svc"), output);
        assertFalse(output.contains("auth-svc"), output);
        assertFalse(output.contains("orphan-svc"), output);
        assertTrue(output.contains("1 row(s)"), output);
    }

    /**
     * Named gap, now fixed: standard SQL's {@code NOT IN} NULL-poisoning
     * rule. {@code customers_with_null} has {@code id=1} and one row
     * with a {@code NULL} id -- every order's {@code customer_id}
     * (1, 2, 999) either matches the real {@code id=1} (excluded
     * normally) or matches nothing, but the subquery's result column
     * contains a {@code NULL}, so standard SQL makes the whole
     * comparison {@code UNKNOWN} for those too -- the correct result is
     * zero rows, not the one row ("orphan-svc") a plain {@code ANTI}
     * join would incorrectly have kept.
     */
    @Test
    void notInSubquery_withANullInTheSubqueryResult_excludesEveryRow(@TempDir Path root) throws Exception {
        registerTables(root);

        QueryCommand.run(new String[] {"--sql",
                "SELECT a.service FROM orders a WHERE a.customer_id NOT IN (SELECT b.id FROM customers_with_null b)"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("0 row(s)"),
                "a NULL in the subquery's result column poisons NOT IN for every non-matching row: " + output);
    }

    /**
     * Same gap, via {@code EXISTS} instead of {@code IN}: the correlation
     * equality ({@code b.id = a.customer_id}) and the nested scalar
     * subquery ({@code b.id = (SELECT MAX(c.id) FROM customers c)}) are
     * both top-level {@code AND}-combined conjuncts inside the same
     * subquery {@code WHERE} -- {@code splitSubqueryWhere} must route
     * each to the right place (a join key vs. a {@code ScalarSubquery}).
     */
    @Test
    void existsSubquery_withAScalarSubqueryInsideItsOwnFilter_resolvesCorrectly(@TempDir Path root) throws Exception {
        registerTables(root);

        QueryCommand.run(new String[] {"--sql",
                "SELECT a.service FROM orders a WHERE EXISTS "
                        + "(SELECT 1 FROM customers b WHERE b.id = a.customer_id "
                        + "AND b.id = (SELECT MAX(c.id) AS m FROM customers c))"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("payments-svc"), output);
        assertFalse(output.contains("auth-svc"), output);
        assertFalse(output.contains("orphan-svc"), output);
        assertTrue(output.contains("1 row(s)"), output);
    }
}
