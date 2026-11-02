package com.labuk.labukdb.core.logical;

import com.labuk.labukdb.core.planner.ComparisonOp;
import com.labuk.labukdb.core.planner.Predicate;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PushFilterIntoScanTest {

    @Test
    void filterDirectlyOnScan_absorbedIntoScanPredicate() {
        Predicate condition = Predicate.compare("level", ComparisonOp.EQ, "ERROR");
        LogicalPlan plan = new Filter(new TableScan("logs", null, null), condition);

        LogicalPlan result = new PushFilterIntoScan().apply(plan);

        TableScan scan = assertInstanceOf(TableScan.class, result, "the Filter node must be removed");
        assertSame(condition, scan.predicate());
    }

    @Test
    void noFilter_leavesPlanUnchanged() {
        LogicalPlan plan = new TableScan("logs", null, null);
        assertSame(plan, new PushFilterIntoScan().apply(plan));
    }

    @Test
    void filterUnderAggregateSortLimit_stillAbsorbedIntoScan() {
        Predicate condition = Predicate.compare("amount", ComparisonOp.GT, 2L);
        LogicalPlan plan = new Limit(
                new Sort(
                        new Aggregate(
                                new Filter(new TableScan("logs", null, null), condition),
                                List.of("level"), List.of()),
                        List.of()),
                3);

        LogicalPlan result = new PushFilterIntoScan().apply(plan);

        Limit limit = assertInstanceOf(Limit.class, result);
        Sort sort = assertInstanceOf(Sort.class, limit.input());
        Aggregate aggregate = assertInstanceOf(Aggregate.class, sort.input());
        TableScan scan = assertInstanceOf(TableScan.class, aggregate.input());
        assertSame(condition, scan.predicate());
    }

    @Test
    void idempotent_onAlreadyOptimizedPlan() {
        Predicate condition = Predicate.compare("level", ComparisonOp.EQ, "ERROR");
        LogicalPlan already = new TableScan("logs", condition, null);

        assertSame(already, new PushFilterIntoScan().apply(already));
    }
}
