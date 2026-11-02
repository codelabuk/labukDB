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
