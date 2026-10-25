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
    void join_rejectedWithClearError() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> SqlQueryTranslator.translate("SELECT a.x FROM a JOIN b ON a.id = b.id"));
        assertTrue(thrown.getMessage().contains("JOIN"), thrown.getMessage());
    }

    @Test
    void unparseableSql_rejectedWithClearError() {
        assertThrows(IllegalArgumentException.class, () -> SqlQueryTranslator.translate("not even sql"));
    }
}
