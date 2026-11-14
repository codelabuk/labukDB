package com.labuk.labukdb.core.query;

import com.labuk.labukdb.core.execution.AggregateFunction;
import com.labuk.labukdb.core.execution.AggregateSpec;
import com.labuk.labukdb.core.execution.JoinType;
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
    void innerEquiJoin_parsesIntoJoinStep() {
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT a.service, b.region FROM orders a JOIN customers b ON a.customer_id = b.id");

        assertEquals("orders", query.table());
        assertEquals("a", query.baseAlias());
        assertEquals(List.of("a.service", "b.region"), query.columns());
        assertEquals(1, query.joins().size());
        SqlQuery.JoinStep step = query.joins().get(0);
        assertEquals(JoinType.INNER, step.type());
        assertEquals("customers", step.rightTable());
        assertEquals("b", step.rightAlias());
        assertEquals(List.of(new SqlQuery.JoinKeyPair("a", "customer_id", "id")), step.keys());
        assertNull(query.predicate());
        assertNull(step.rightPredicate());
    }

    @Test
    void innerEquiJoin_withMultiKeyAndPerSideWhere_splitsCorrectly() {
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT a.service FROM orders a JOIN customers b "
                        + "ON a.customer_id = b.id AND a.region = b.region "
                        + "WHERE a.amount > 10 AND b.active = 'Y'");

        SqlQuery.JoinStep step = query.joins().get(0);
        assertEquals(List.of(new SqlQuery.JoinKeyPair("a", "customer_id", "id"),
                new SqlQuery.JoinKeyPair("a", "region", "region")), step.keys());
        assertNotNull(query.predicate(), "a.amount > 10 must route to the base table");
        assertNotNull(step.rightPredicate(), "b.active = 'Y' must route to the new table");
    }

    @Test
    void leftRightFullJoin_allParseWithTheirOwnType() {
        assertEquals(JoinType.LEFT, SqlQueryTranslator.translate(
                "SELECT a.x FROM a LEFT JOIN b ON a.id = b.id").joins().get(0).type());
        assertEquals(JoinType.RIGHT, SqlQueryTranslator.translate(
                "SELECT a.x FROM a RIGHT JOIN b ON a.id = b.id").joins().get(0).type());
        assertEquals(JoinType.FULL, SqlQueryTranslator.translate(
                "SELECT a.x FROM a FULL JOIN b ON a.id = b.id").joins().get(0).type());
        assertEquals(JoinType.FULL, SqlQueryTranslator.translate(
                "SELECT a.x FROM a FULL OUTER JOIN b ON a.id = b.id").joins().get(0).type());
    }

    @Test
    void crossJoin_stillRejectedWithClearError() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> SqlQueryTranslator.translate("SELECT a.x FROM a CROSS JOIN b"));
        assertTrue(thrown.getMessage().contains("CROSS"), thrown.getMessage());
    }

    @Test
    void threeWayJoin_parsesAsALeftDeepChain() {
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT a.x, b.y, c.z FROM a JOIN b ON a.id = b.a_id JOIN c ON c.b_id = b.id");

        assertEquals(2, query.joins().size());
        SqlQuery.JoinStep first = query.joins().get(0);
        assertEquals("b", first.rightAlias());
        assertEquals(List.of(new SqlQuery.JoinKeyPair("a", "id", "a_id")), first.keys());

        SqlQuery.JoinStep second = query.joins().get(1);
        assertEquals("c", second.rightAlias());
        // ON c.b_id = b.id -- b was introduced by the FIRST step, not the base
        // table, so this proves a later step can key against any earlier one.
        assertEquals(List.of(new SqlQuery.JoinKeyPair("b", "id", "b_id")), second.keys());
    }

    @Test
    void leftJoin_whereOnNonPreservedSide_rejectedWithClearError() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> SqlQueryTranslator.translate(
                        "SELECT a.x FROM a LEFT JOIN b ON a.id = b.id WHERE b.active = 'Y'"));
        assertTrue(thrown.getMessage().contains("null-padded") || thrown.getMessage().contains("safely"), thrown.getMessage());
    }

    @Test
    void leftJoin_whereOnPreservedSide_isAccepted() {
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT a.x FROM a LEFT JOIN b ON a.id = b.id WHERE a.active = 'Y'");
        assertNotNull(query.predicate(), "a WHERE condition on the preserved (left) side is always safe to push");
    }

    @Test
    void fullJoin_whereOnEitherSide_rejectedWithClearError() {
        assertThrows(IllegalArgumentException.class, () -> SqlQueryTranslator.translate(
                "SELECT a.x FROM a FULL JOIN b ON a.id = b.id WHERE a.active = 'Y'"));
        assertThrows(IllegalArgumentException.class, () -> SqlQueryTranslator.translate(
                "SELECT a.x FROM a FULL JOIN b ON a.id = b.id WHERE b.active = 'Y'"));
    }

    @Test
    void rightJoin_taintsTheAccumulatedSideForALaterStepToo() {
        // a RIGHT JOIN b makes 'a' (accumulated-so-far) the non-preserved
        // side; a WHERE on 'a' must be rejected even though 'a' is the base table.
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> SqlQueryTranslator.translate(
                        "SELECT a.x FROM a RIGHT JOIN b ON a.id = b.id WHERE a.active = 'Y'"));
        assertTrue(thrown.getMessage().contains("null-padded") || thrown.getMessage().contains("safely"), thrown.getMessage());
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

        assertEquals(1, query.joins().size());
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

        assertEquals(1, query.joins().size());
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

    // --- SQL engine Phase 4, increment 1: IN/NOT IN/EXISTS/NOT EXISTS subqueries ---

    @Test
    void uncorrelatedInSubquery_noExplicitJoin_decorrelatesIntoASemiStep() {
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT a.x FROM a WHERE a.y IN (SELECT b.z FROM b)");

        assertEquals("a", query.baseAlias(), "a subquery condition routes through the join-shaped path even with no explicit JOIN");
        assertEquals(1, query.joins().size());
        SqlQuery.JoinStep step = query.joins().get(0);
        assertEquals(JoinType.SEMI, step.type());
        assertEquals("b", step.rightTable());
        assertEquals("b", step.rightAlias());
        assertEquals(List.of(new SqlQuery.JoinKeyPair("a", "y", "z")), step.keys());
        assertNull(step.rightPredicate(), "no extra filter inside the subquery beyond the IN's own match");
    }

    @Test
    void negatedInSubquery_decorrelatesIntoAnAntiStep() {
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT a.x FROM a WHERE a.y NOT IN (SELECT b.z FROM b)");
        assertEquals(JoinType.ANTI, query.joins().get(0).type());
    }

    @Test
    void correlatedExistsSubquery_decorrelatesIntoASemiStepWithCorrelationKeyAndOwnFilter() {
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT a.x FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.aid = a.id AND b.active = 'Y')");

        SqlQuery.JoinStep step = query.joins().get(0);
        assertEquals(JoinType.SEMI, step.type());
        assertEquals(List.of(new SqlQuery.JoinKeyPair("a", "id", "aid")), step.keys(),
                "the correlation equality (b.aid = a.id) becomes the step's join key");
        assertNotNull(step.rightPredicate(), "b.active = 'Y' is the subquery's own filter, not a correlation");
    }

    @Test
    void negatedExistsSubquery_decorrelatesIntoAnAntiStep() {
        // Confirmed via a real parse this session: "NOT EXISTS (...)" is a
        // NotExpression wrapping an ExistsExpression, NOT ExistsExpression's
        // own isNot() flag -- the thing this test actually proves.
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT a.x FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.aid = a.id)");
        assertEquals(JoinType.ANTI, query.joins().get(0).type());
    }

    @Test
    void existsSubquery_withNoCorrelationAtAll_rejectedWithClearError() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> SqlQueryTranslator.translate(
                        "SELECT a.x FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.active = 'Y')"));
        assertTrue(thrown.getMessage().contains("correlation"), thrown.getMessage());
    }

    @Test
    void existsSubquery_nonEqualityConditionOnly_treatedAsOwnFilterNotCorrelation_thenRejectedForNoCorrelation() {
        // b.x > a.y is not a plain equality -- splitSubqueryWhere must not
        // mistake it for a correlation key; with nothing else, this EXISTS
        // ends up with zero correlation keys and must hit the same
        // key-less rejection as a wholly uncorrelated EXISTS.
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> SqlQueryTranslator.translate(
                        "SELECT a.x FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.x > a.y)"));
        assertTrue(thrown.getMessage().contains("correlation"), thrown.getMessage());
    }

    @Test
    void inSubquery_selectingMoreThanOneColumn_rejectedWithClearError() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> SqlQueryTranslator.translate(
                        "SELECT a.x FROM a WHERE a.y IN (SELECT b.z, b.w FROM b)"));
        assertTrue(thrown.getMessage().contains("exactly one"), thrown.getMessage());
    }

    @Test
    void existsSubquery_withItsOwnJoinInside_rejectedWithClearError() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> SqlQueryTranslator.translate(
                        "SELECT a.x FROM a WHERE EXISTS (SELECT 1 FROM b JOIN c ON b.id = c.id WHERE b.aid = a.id)"));
        assertTrue(thrown.getMessage().contains("JOIN"), thrown.getMessage());
    }

    @Test
    void scalarSubqueryAsComparisonOperand_rejectedWithDedicatedError_notUnsupportedLiteral() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> SqlQueryTranslator.translate("SELECT a.x FROM a WHERE a.y = (SELECT b.z FROM b)"));
        assertTrue(thrown.getMessage().contains("Scalar subquer"), thrown.getMessage());
    }

    @Test
    void existsSubquery_afterAnExplicitJoin_canCorrelateAgainstTheExplicitJoinsOwnTable() {
        // The subquery step must append AFTER the explicit join, so "c" (the
        // explicit join's own alias) is already in scope for correlation.
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT a.x FROM a JOIN c ON a.id = c.aid "
                        + "WHERE EXISTS (SELECT 1 FROM b WHERE b.cid = c.id)");

        assertEquals(2, query.joins().size());
        assertEquals(JoinType.INNER, query.joins().get(0).type());
        SqlQuery.JoinStep subqueryStep = query.joins().get(1);
        assertEquals(JoinType.SEMI, subqueryStep.type());
        assertEquals(List.of(new SqlQuery.JoinKeyPair("c", "id", "cid")), subqueryStep.keys());
    }

    @Test
    void subqueryAlias_collidingWithAnOuterTable_rejectedWithClearError() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> SqlQueryTranslator.translate(
                        "SELECT a.x FROM a WHERE EXISTS (SELECT 1 FROM a WHERE a.y = a.z)"));
        assertTrue(thrown.getMessage().contains("alias"), thrown.getMessage());
    }
}
