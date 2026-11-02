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
}
