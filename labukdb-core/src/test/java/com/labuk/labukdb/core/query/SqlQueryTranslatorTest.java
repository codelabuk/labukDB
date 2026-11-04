package com.labuk.labukdb.core.query;

import com.labuk.labukdb.core.execution.AggregateFunction;
import com.labuk.labukdb.core.execution.AggregateSpec;
import com.labuk.labukdb.core.execution.SortKey;
import com.labuk.labukdb.core.planner.ComparisonOp;
import com.labuk.labukdb.core.planner.Predicate;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Proves {@link SqlQueryTranslator} maps real SQL text onto exactly the
 * structures the engine already executes correctly, and rejects every
 * construct outside the documented subset with a clear, specific error
 * rather than a confusing failure or a silent misinterpretation.
 */
class SqlQueryTranslatorTest {

    @Test
    void simpleSelect_withWhere() {
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT service, amount FROM logs WHERE level = 'ERROR' AND amount > 2");

        assertEquals("logs", query.table());
        assertEquals(List.of("service", "amount"), query.columns());
        assertTrue(query.groupByColumns().isEmpty());
        assertTrue(query.aggregates().isEmpty());
        assertEquals(-1, query.limit());

        Predicate.And and = (Predicate.And) query.predicate();
        assertEquals(2, and.children.size());
        Predicate.Comparison left = (Predicate.Comparison) and.children.get(0);
        assertEquals("level", left.column);
        assertEquals(ComparisonOp.EQ, left.op);
        assertEquals("ERROR", left.literal);
        Predicate.Comparison right = (Predicate.Comparison) and.children.get(1);
        assertEquals("amount", right.column);
        assertEquals(ComparisonOp.GT, right.op);
        assertEquals(2L, right.literal);
    }

    @Test
    void groupBy_withMultipleAggregatesAndAliases() {
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT level, COUNT(amount) AS cnt, SUM(amount) AS total, AVG(amount) AS avg_amt "
                        + "FROM logs GROUP BY level");

        assertEquals(List.of("level"), query.groupByColumns());
        assertEquals(List.of(
                new AggregateSpec("amount", AggregateFunction.COUNT, "cnt"),
                new AggregateSpec("amount", AggregateFunction.SUM, "total"),
                new AggregateSpec("amount", AggregateFunction.AVG, "avg_amt")
        ), query.aggregates());
        assertTrue(query.columns().isEmpty(), "no separate projection list once aggregating");
    }

    /**
     * {@code MIN}/{@code MAX} are fully implemented ({@code
     * AggregateFunction}/{@code AggregateOperator}) but had zero coverage
     * through actual SQL text until this session's SQL engine Phase 1
     * research found the gap — closing it here.
     */
    @Test
    void groupBy_withMinAndMax() {
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT level, MIN(amount) AS min_amt, MAX(amount) AS max_amt FROM logs GROUP BY level");

        assertEquals(List.of("level"), query.groupByColumns());
        assertEquals(List.of(
                new AggregateSpec("amount", AggregateFunction.MIN, "min_amt"),
                new AggregateSpec("amount", AggregateFunction.MAX, "max_amt")
        ), query.aggregates());
    }

    @Test
    void orderByLimit_withNoGroupBy() {
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT amount FROM logs ORDER BY amount DESC LIMIT 3");

        assertTrue(query.groupByColumns().isEmpty());
        assertEquals(List.of(new SortKey("amount", false)), query.orderBy());
        assertEquals(3, query.limit());
    }

    @Test
    void groupByOrderByLimit_allTogether() {
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT level, SUM(amount) AS total FROM logs GROUP BY level ORDER BY total DESC LIMIT 1");

        assertEquals(List.of("level"), query.groupByColumns());
        assertEquals(1, query.aggregates().size());
        assertEquals(List.of(new SortKey("total", false)), query.orderBy());
        assertEquals(1, query.limit());
    }

    @Test
    void inAndLikePredicates() {
        // Explicit parentheses are required here: jsqlparser's IN grammar production
        // greedily consumes a trailing OR/AND as its right-hand expression instead of
        // binding tighter than OR (confirmed via a real parse of the unparenthesized
        // form), so "level IN (...) OR service LIKE ..." without parens is genuinely
        // ambiguous and is covered separately by inWithoutParensBeforeOr_rejectedWithClearError.
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT service FROM logs WHERE (level IN ('ERROR', 'WARN')) OR (service LIKE 'auth%')");

        Predicate.Or or = (Predicate.Or) query.predicate();
        assertEquals(2, or.children.size());
        assertInstanceOf(Predicate.In.class, or.children.get(0));
        assertInstanceOf(Predicate.Like.class, or.children.get(1));
    }

    @Test
    void inWithoutParensBeforeOr_rejectedWithClearError() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> SqlQueryTranslator.translate(
                        "SELECT service FROM logs WHERE level IN ('ERROR', 'WARN') OR service LIKE 'auth%'"));
        assertTrue(thrown.getMessage().contains("parenthes"), thrown.getMessage());
    }

    @Test
    void noWhereClause_predicateIsNull() {
        SqlQuery query = SqlQueryTranslator.translate("SELECT service FROM logs");
        assertNull(query.predicate());
    }

    @Test
    void not_rejectedWithClearError() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> SqlQueryTranslator.translate("SELECT service FROM logs WHERE NOT level = 'ERROR'"));
        assertTrue(thrown.getMessage().toLowerCase().contains("not") || thrown.getMessage().contains("Unsupported"),
                thrown.getMessage());
    }

    @Test
    void notIn_rejectedWithClearError() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> SqlQueryTranslator.translate("SELECT service FROM logs WHERE level NOT IN ('ERROR')"));
        assertTrue(thrown.getMessage().contains("NOT IN"), thrown.getMessage());
    }

    @Test
    void countStar_rejectedWithClearError() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> SqlQueryTranslator.translate("SELECT COUNT(*) AS cnt FROM logs"));
        assertTrue(thrown.getMessage().contains("COUNT(column)"), thrown.getMessage());
    }

    @Test
    void aggregateWithoutAlias_rejectedWithClearError() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> SqlQueryTranslator.translate("SELECT level, COUNT(amount) FROM logs GROUP BY level"));
        assertTrue(thrown.getMessage().contains("alias"), thrown.getMessage());
    }

    @Test
    void innerEquiJoin_parsesIntoJoinClause() {
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT a.service, b.region FROM orders a JOIN customers b ON a.customer_id = b.id");

        assertEquals("orders", query.table());
        assertEquals(List.of("a.service", "b.region"), query.columns());
        assertNotNull(query.join());
        SqlQuery.JoinClause join = query.join();
        assertEquals("a", join.leftAlias());
        assertEquals("customers", join.rightTable());
        assertEquals("b", join.rightAlias());
        assertEquals(List.of(new SqlQuery.JoinKeyPair("customer_id", "id")), join.keys());
        assertNull(join.leftPredicate());
        assertNull(join.rightPredicate());
    }

    @Test
    void innerEquiJoin_withMultiKeyAndPerSideWhere_splitsCorrectly() {
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT a.service FROM orders a JOIN customers b "
                        + "ON a.customer_id = b.id AND a.region = b.region "
                        + "WHERE a.amount > 10 AND b.active = 'Y'");

        SqlQuery.JoinClause join = query.join();
        assertEquals(List.of(new SqlQuery.JoinKeyPair("customer_id", "id"),
                new SqlQuery.JoinKeyPair("region", "region")), join.keys());
        assertNotNull(join.leftPredicate(), "a.amount > 10 must route to the left side");
        assertNotNull(join.rightPredicate(), "b.active = 'Y' must route to the right side");
    }

    @Test
    void leftJoin_rejectedWithClearError() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> SqlQueryTranslator.translate(
                        "SELECT a.x FROM a LEFT JOIN b ON a.id = b.id"));
        assertTrue(thrown.getMessage().contains("INNER"), thrown.getMessage());
    }

    @Test
    void nonEquiJoinCondition_rejectedWithClearError() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> SqlQueryTranslator.translate(
                        "SELECT a.x FROM a JOIN b ON a.id > b.id"));
        assertTrue(thrown.getMessage().contains("equality"), thrown.getMessage());
    }

    @Test
    void unqualifiedSelectColumnWithJoin_rejectedWithClearError() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> SqlQueryTranslator.translate(
                        "SELECT x FROM a JOIN b ON a.id = b.id"));
        assertTrue(thrown.getMessage().contains("qualified"), thrown.getMessage());
    }

    @Test
    void crossTableWhereCondition_rejectedWithClearError() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> SqlQueryTranslator.translate(
                        "SELECT a.x FROM a JOIN b ON a.id = b.id WHERE a.x = b.y"));
        assertTrue(thrown.getMessage().contains("ON"), thrown.getMessage());
    }

    /** SQL engine Phase 3, increment 2: GROUP BY/aggregates combined with a JOIN is now supported. */
    @Test
    void joinWithGroupByAndAggregate_parsesCorrectlyWithQualifiedColumns() {
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT a.region, SUM(b.amount) AS total FROM orders a JOIN customers b "
                        + "ON a.customer_id = b.id GROUP BY a.region");

        assertNotNull(query.join());
        assertEquals(List.of(), query.columns(), "plain SELECT columns are irrelevant once GROUP BY is present");
        assertEquals(List.of("a.region"), query.groupByColumns());
        assertEquals(List.of(new AggregateSpec("b.amount", AggregateFunction.SUM, "total")), query.aggregates());
    }

    /** SQL engine Phase 3, increment 2: ORDER BY/LIMIT combined with a JOIN (no GROUP BY) is now supported. */
    @Test
    void joinWithOrderByAndLimit_parsesCorrectlyWithQualifiedColumns() {
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT a.service, b.region FROM orders a JOIN customers b "
                        + "ON a.customer_id = b.id ORDER BY b.region DESC LIMIT 5");

        assertNotNull(query.join());
        assertEquals(List.of("a.service", "b.region"), query.columns());
        assertEquals(List.of(new SortKey("b.region", false)), query.orderBy());
        assertEquals(5L, query.limit());
    }

    @Test
    void unqualifiedGroupByColumnWithJoin_rejectedWithClearError() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> SqlQueryTranslator.translate(
                        "SELECT a.region, SUM(a.x) AS total FROM a JOIN b ON a.id = b.id GROUP BY region"));
        assertTrue(thrown.getMessage().contains("qualified"), thrown.getMessage());
    }

    @Test
    void unqualifiedOrderByColumnWithJoin_rejectedWithClearError() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> SqlQueryTranslator.translate(
                        "SELECT a.x FROM a JOIN b ON a.id = b.id ORDER BY x"));
        assertTrue(thrown.getMessage().contains("qualified"), thrown.getMessage());
    }

    @Test
    void unparseableSql_rejectedWithClearError() {
        assertThrows(IllegalArgumentException.class, () -> SqlQueryTranslator.translate("not even sql"));
    }
}
