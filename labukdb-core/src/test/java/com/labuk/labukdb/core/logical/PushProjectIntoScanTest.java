package com.labuk.labukdb.core.logical;

import com.labuk.labukdb.core.execution.AggregateFunction;
import com.labuk.labukdb.core.execution.AggregateSpec;
import com.labuk.labukdb.core.execution.SortKey;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PushProjectIntoScanTest {

    @Test
    void plainSelect_pushesSelectColumnsIntoScanAndDropsProject() {
        LogicalPlan plan = new Project(new TableScan("logs", null, null), List.of("service", "amount"));

        LogicalPlan result = new PushProjectIntoScan().apply(plan);

        TableScan scan = assertInstanceOf(TableScan.class, result, "Project must be eliminated for a single-table query");
        assertEquals(List.of("service", "amount"), scan.requestedColumns());
    }

    @Test
    void withOrderBy_unionsSortColumnsIntoScan() {
        LogicalPlan plan = new Project(
                new Sort(new TableScan("logs", null, null), List.of(new SortKey("amount", false))),
                List.of("amount"));

        LogicalPlan result = new PushProjectIntoScan().apply(plan);

        Sort sort = assertInstanceOf(Sort.class, result);
        TableScan scan = assertInstanceOf(TableScan.class, sort.input());
        assertEquals(List.of("amount"), scan.requestedColumns());
    }

    @Test
    void withAggregate_ignoresSelectColumnsUsesGroupByAndAggregateInputOnly() {
        LogicalPlan plan = new Project(
                new Aggregate(new TableScan("logs", null, null),
                        List.of("level"), List.of(new AggregateSpec("amount", AggregateFunction.COUNT, "cnt"))),
                List.of()); // SqlQueryTranslator leaves columns() empty once aggregating

        LogicalPlan result = new PushProjectIntoScan().apply(plan);

        Aggregate aggregate = assertInstanceOf(Aggregate.class, result);
        TableScan scan = assertInstanceOf(TableScan.class, aggregate.input());
        assertEquals(List.of("level", "amount"), scan.requestedColumns());
    }

    @Test
    void noProjectAtRoot_leavesPlanUnchanged() {
        LogicalPlan plan = new TableScan("logs", null, List.of("amount"));
        assertSame(plan, new PushProjectIntoScan().apply(plan));
    }

    /**
     * SQL engine Phase 3, increment 2 -- the real bug this increment found
     * and fixed: without an Aggregate present, Project must survive no
     * matter how deep the Join sits (Sort/Limit between Project and Join),
     * since the join's combined columns are still wider than the real
     * SELECT list. Before the fix, this exact shape silently eliminated
     * Project because the old check only recognized a *bare* Join child.
     */
    @Test
    void joinWithOrderByAndLimit_keepsProjectEvenThoughJoinIsNotTheImmediateChild() {
        LogicalPlan join = new Join(new TableScan("orders", null, null), new TableScan("customers", null, null),
                List.of(new JoinKey("customer_id", "id")));
        LogicalPlan plan = new Project(
                new Limit(new Sort(join, List.of(new SortKey("b.region", false))), 5),
                List.of("a.service", "b.region"));

        LogicalPlan result = new PushProjectIntoScan().apply(plan);

        Project project = assertInstanceOf(Project.class, result, "Project must survive -- the join's combined "
                + "columns are still wider than the real SELECT list");
        assertEquals(List.of("a.service", "b.region"), project.columns());
        Limit limit = assertInstanceOf(Limit.class, project.input());
        Sort sort = assertInstanceOf(Sort.class, limit.input());
        assertSame(join, sort.input(), "the Join itself is untouched -- column pruning still isn't pushed across it");
    }

    /** Join + Aggregate: Project is still eliminated, same as the no-join aggregate case -- Aggregate's own output is already the final shape. */
    @Test
    void joinWithGroupByAndAggregate_stillEliminatesProject() {
        LogicalPlan join = new Join(new TableScan("orders", null, null), new TableScan("customers", null, null),
                List.of(new JoinKey("customer_id", "id")));
        LogicalPlan plan = new Project(
                new Aggregate(join, List.of("a.region"), List.of(new AggregateSpec("b.amount", AggregateFunction.SUM, "total"))),
                List.of()); // SqlQueryTranslator leaves columns() empty once aggregating, join or not

        LogicalPlan result = new PushProjectIntoScan().apply(plan);

        Aggregate aggregate = assertInstanceOf(Aggregate.class, result, "Project must be eliminated once Aggregate is present, even with a Join underneath");
        assertSame(join, aggregate.input(), "the Join itself is untouched");
    }
}
