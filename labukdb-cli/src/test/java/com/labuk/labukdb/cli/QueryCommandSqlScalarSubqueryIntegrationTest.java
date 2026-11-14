package com.labuk.labukdb.cli;

import com.labuk.labukdb.core.catalog.TableCatalog;
import com.labuk.labukdb.core.log.CommitWriter;
import com.labuk.labukdb.core.log.TableCreator;
import com.labuk.labukdb.core.storage.TableLocation;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
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
 * SQL engine Phase 4 increment 2: a scalar subquery used as a comparison
 * operand ({@code WHERE x OP (SELECT ...)}), end to end through {@code
 * --sql}. Unlike {@code IN}/{@code EXISTS} ({@link
 * QueryCommandSqlSubqueryIntegrationTest}), this never becomes a {@code
 * Join} step -- {@code QueryCommand.resolveScalarSubqueries} runs each one
 * directly, via the exact single-table primitives a top-level query
 * already uses, and substitutes its single value before the outer plan is
 * ever built.
 */
class QueryCommandSqlScalarSubqueryIntegrationTest {

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

    /** orders: id(long), amount(long). id 1,2,3 -- amounts 100, 150, 200. */
    private void buildOrders(Path tableRoot) throws Exception {
        Field id = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field amount = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Schema schema = new Schema(List.of(id, amount));
        TableLocation location = TableLocation.forLocal(tableRoot);
        new TableCreator().createTable(location, schema, List.of(), null);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
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

    /** limits: id(long), max_amount(long). Exactly one row: {1, 150}. */
    private void buildLimits(Path tableRoot) throws Exception {
        Field id = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field maxAmount = new Field("max_amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Schema schema = new Schema(List.of(id, maxAmount));
        TableLocation location = TableLocation.forLocal(tableRoot);
        new TableCreator().createTable(location, schema, List.of(), null);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator)) {
            batch.allocateNew();
            ((BigIntVector) batch.getVector("id")).setSafe(0, 1L);
            ((BigIntVector) batch.getVector("max_amount")).setSafe(0, 150L);
            batch.setRowCount(1);
            CommitWriter.forLocation(location).appendBatch(batch, Map.of());
        }
    }

    private void registerTables(Path root) throws Exception {
        buildOrders(root.resolve("orders"));
        buildLimits(root.resolve("limits"));
        TableCatalog.atDefaultLocation().register("orders", root.resolve("orders").toAbsolutePath().toString());
        TableCatalog.atDefaultLocation().register("limits", root.resolve("limits").toAbsolutePath().toString());
    }

    @Test
    void plainColumnScalarSubquery_matchesExactlyOneRow(@TempDir Path root) throws Exception {
        registerTables(root);

        QueryCommand.run(new String[] {"--sql",
                "SELECT o.id FROM orders o WHERE o.amount = (SELECT l.max_amount FROM limits l)"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("1 row(s)"), output);
        List<String> dataLines = output.lines().dropWhile(l -> !l.startsWith("---")).skip(1)
                .takeWhile(l -> !l.isBlank()).toList();
        assertEquals(List.of("2"), dataLines, "only id=2 has amount=150, matching the single limits row: " + output);
    }

    @Test
    void aggregateScalarSubquery_noGroupBy_matchesTheMaxRow(@TempDir Path root) throws Exception {
        registerTables(root);

        QueryCommand.run(new String[] {"--sql",
                "SELECT o.id FROM orders o WHERE o.amount = (SELECT MAX(o2.amount) AS m FROM orders o2)"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("1 row(s)"), output);
        List<String> dataLines = output.lines().dropWhile(l -> !l.startsWith("---")).skip(1)
                .takeWhile(l -> !l.isBlank()).toList();
        assertEquals(List.of("3"), dataLines, "id=3 has amount=200, the max: " + output);
    }

    @Test
    void zeroRowScalarSubquery_shortCircuitsTheWholeOuterQueryToZeroRows(@TempDir Path root) throws Exception {
        registerTables(root);

        QueryCommand.run(new String[] {"--sql",
                "SELECT o.id FROM orders o WHERE o.amount = "
                        + "(SELECT l.max_amount FROM limits l WHERE l.id = 999)"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("0 row(s)"), "x = NULL is never true, for any operator: " + output);
    }

    @Test
    void moreThanOneRowScalarSubquery_rejectedWithClearError(@TempDir Path root) throws Exception {
        registerTables(root);

        Exception thrown = assertThrows(Exception.class, () -> QueryCommand.run(new String[] {"--sql",
                "SELECT o.id FROM orders o WHERE o.amount = (SELECT o2.amount FROM orders o2)"}));
        assertTrue(thrown.getMessage().contains("more than one row"), thrown.getMessage());
    }

    @Test
    void scalarSubqueryWithItsOwnJoin_rejectedWithClearError(@TempDir Path root) throws Exception {
        registerTables(root);

        Exception thrown = assertThrows(Exception.class, () -> QueryCommand.run(new String[] {"--sql",
                "SELECT o.id FROM orders o WHERE o.amount = "
                        + "(SELECT c.amount FROM orders c JOIN limits l ON c.id = l.id)"}));
        assertTrue(thrown.getMessage().contains("JOIN"), thrown.getMessage());
    }

    @Test
    void scalarSubqueryWithGroupBy_rejectedWithClearError(@TempDir Path root) throws Exception {
        registerTables(root);

        Exception thrown = assertThrows(Exception.class, () -> QueryCommand.run(new String[] {"--sql",
                "SELECT o.id FROM orders o WHERE o.amount = "
                        + "(SELECT MAX(o2.amount) AS m FROM orders o2 GROUP BY o2.id)"}));
        assertTrue(thrown.getMessage().contains("GROUP BY"), thrown.getMessage());
    }

    @Test
    void nestedScalarSubquery_resolvesRecursively(@TempDir Path root) throws Exception {
        registerTables(root);

        // Outer: o.amount = (inner: o2.amount = (innermost: MAX(o3.amount))) --
        // innermost resolves to 200 first, inner then finds the orders row
        // whose amount is 200 (itself 200), outer then matches that same value.
        QueryCommand.run(new String[] {"--sql",
                "SELECT o.id FROM orders o WHERE o.amount = "
                        + "(SELECT o2.amount FROM orders o2 WHERE o2.amount = "
                        + "(SELECT MAX(o3.amount) AS m FROM orders o3))"});

        String output = capturedOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("1 row(s)"), output);
        List<String> dataLines = output.lines().dropWhile(l -> !l.startsWith("---")).skip(1)
                .takeWhile(l -> !l.isBlank()).toList();
        assertEquals(List.of("3"), dataLines, output);
    }
}
