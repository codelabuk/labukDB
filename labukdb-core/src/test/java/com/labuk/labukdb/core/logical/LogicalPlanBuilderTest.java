package com.labuk.labukdb.core.logical;

import com.labuk.labukdb.core.query.SqlQuery;
import com.labuk.labukdb.core.query.SqlQueryTranslator;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Proves {@link LogicalPlanBuilder} wraps an already-translated {@link SqlQuery} into the right tree shape. */
class LogicalPlanBuilderTest {

    @Test
    void plainSelect_buildsScanWrappedInProjectOnly() {
        SqlQuery query = SqlQueryTranslator.translate("SELECT service, amount FROM logs");
        LogicalPlan plan = LogicalPlanBuilder.build(query);

        Project project = assertInstanceOf(Project.class, plan);
        assertEquals(List.of("service", "amount"), project.columns());
        TableScan scan = assertInstanceOf(TableScan.class, project.input());
        assertEquals("logs", scan.tableName());
        assertNull(scan.predicate());
        assertNull(scan.requestedColumns());
    }

    @Test
    void withWhere_wrapsFilterDirectlyOnScan() {
        SqlQuery query = SqlQueryTranslator.translate("SELECT service FROM logs WHERE amount > 2");
        LogicalPlan plan = LogicalPlanBuilder.build(query);

        Project project = assertInstanceOf(Project.class, plan);
        Filter filter = assertInstanceOf(Filter.class, project.input());
        assertNotNull(filter.condition());
        assertInstanceOf(TableScan.class, filter.input());
    }

    @Test
    void groupByWithAggregates_wrapsAggregateDirectlyOnScan() {
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT level, COUNT(amount) AS cnt FROM logs GROUP BY level");
        LogicalPlan plan = LogicalPlanBuilder.build(query);

        Project project = assertInstanceOf(Project.class, plan);
        Aggregate aggregate = assertInstanceOf(Aggregate.class, project.input());
        assertEquals(List.of("level"), aggregate.groupByColumns());
        assertEquals(1, aggregate.aggregates().size());
        assertInstanceOf(TableScan.class, aggregate.input());
    }

    @Test
    void orderByLimit_wrapsLimitOverSortOverScan() {
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT amount FROM logs ORDER BY amount DESC LIMIT 3");
        LogicalPlan plan = LogicalPlanBuilder.build(query);

        Project project = assertInstanceOf(Project.class, plan);
        Limit limit = assertInstanceOf(Limit.class, project.input());
        assertEquals(3, limit.limit());
        Sort sort = assertInstanceOf(Sort.class, limit.input());
        assertEquals(1, sort.sortKeys().size());
        assertInstanceOf(TableScan.class, sort.input());
    }

    /** SQL engine Phase 3, increment 1: {@code Project(Join(Filter(TableScan), Filter(TableScan)))}. */
    @Test
    void innerJoinWithPerSideWhere_buildsJoinWithFilterOnEachScan() {
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT a.service, b.region FROM orders a JOIN customers b "
                        + "ON a.customer_id = b.id WHERE a.amount > 10 AND b.active = 'Y'");
        LogicalPlan plan = LogicalPlanBuilder.build(query);

        Project project = assertInstanceOf(Project.class, plan);
        assertEquals(List.of("a.service", "b.region"), project.columns());
        Join join = assertInstanceOf(Join.class, project.input());
        assertEquals(List.of(new JoinKey("customer_id", "id")), join.keys());

        Filter leftFilter = assertInstanceOf(Filter.class, join.left());
        assertNotNull(leftFilter.condition());
        TableScan leftScan = assertInstanceOf(TableScan.class, leftFilter.input());
        assertEquals("orders", leftScan.tableName());

        Filter rightFilter = assertInstanceOf(Filter.class, join.right());
        assertNotNull(rightFilter.condition());
        TableScan rightScan = assertInstanceOf(TableScan.class, rightFilter.input());
        assertEquals("customers", rightScan.tableName());
    }

    @Test
    void innerJoinWithNoWhere_bothSidesAreBareScans() {
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT a.service, b.region FROM orders a JOIN customers b ON a.customer_id = b.id");
        LogicalPlan plan = LogicalPlanBuilder.build(query);

        Project project = assertInstanceOf(Project.class, plan);
        Join join = assertInstanceOf(Join.class, project.input());
        assertInstanceOf(TableScan.class, join.left());
        assertInstanceOf(TableScan.class, join.right());
    }

    /** SQL engine Phase 3, increment 2: {@code Project(Aggregate(Join(...)))}. */
    @Test
    void innerJoinWithGroupByAndAggregate_wrapsAggregateDirectlyOnJoin() {
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT a.region, SUM(b.amount) AS total FROM orders a JOIN customers b "
                        + "ON a.customer_id = b.id GROUP BY a.region");
        LogicalPlan plan = LogicalPlanBuilder.build(query);

        Project project = assertInstanceOf(Project.class, plan);
        assertEquals(List.of(), project.columns());
        Aggregate aggregate = assertInstanceOf(Aggregate.class, project.input());
        assertEquals(List.of("a.region"), aggregate.groupByColumns());
        assertEquals(1, aggregate.aggregates().size());
        assertInstanceOf(Join.class, aggregate.input());
    }

    /** SQL engine Phase 3, increment 2: {@code Project(Limit(Sort(Join(...))))}. */
    @Test
    void innerJoinWithOrderByAndLimit_wrapsLimitOverSortOverJoin() {
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT a.service, b.region FROM orders a JOIN customers b "
                        + "ON a.customer_id = b.id ORDER BY b.region DESC LIMIT 5");
        LogicalPlan plan = LogicalPlanBuilder.build(query);

        Project project = assertInstanceOf(Project.class, plan);
        assertEquals(List.of("a.service", "b.region"), project.columns());
        Limit limit = assertInstanceOf(Limit.class, project.input());
        assertEquals(5, limit.limit());
        Sort sort = assertInstanceOf(Sort.class, limit.input());
        assertEquals(List.of(new com.labuk.labukdb.core.execution.SortKey("b.region", false)), sort.sortKeys());
        assertInstanceOf(Join.class, sort.input());
    }

    @Test
    void everyClauseCombined_nestsInTheExpectedOrder() {
        SqlQuery query = SqlQueryTranslator.translate(
                "SELECT level, SUM(amount) AS total FROM logs GROUP BY level ORDER BY total DESC LIMIT 1");
        LogicalPlan plan = LogicalPlanBuilder.build(query);

        Project project = assertInstanceOf(Project.class, plan);
        Limit limit = assertInstanceOf(Limit.class, project.input());
        Sort sort = assertInstanceOf(Sort.class, limit.input());
        Aggregate aggregate = assertInstanceOf(Aggregate.class, sort.input());
        assertInstanceOf(TableScan.class, aggregate.input());
    }
}
