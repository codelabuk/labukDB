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

    /** SQL engine Phase 3, increment 1: each side's own Filter absorbs into its own TableScan independently. */
    @Test
    void joinWithPerSideFilters_absorbsEachIndependently() {
        Predicate leftCondition = Predicate.compare("amount", ComparisonOp.GT, 10L);
        Predicate rightCondition = Predicate.compare("active", ComparisonOp.EQ, "Y");
        LogicalPlan plan = new Join(
                new Filter(new TableScan("orders", null, null), leftCondition),
                new Filter(new TableScan("customers", null, null), rightCondition),
                List.of(new JoinKey("customer_id", "id")));

        LogicalPlan result = new PushFilterIntoScan().apply(plan);

        Join join = assertInstanceOf(Join.class, result);
        TableScan leftScan = assertInstanceOf(TableScan.class, join.left(), "left Filter must be absorbed");
        assertSame(leftCondition, leftScan.predicate());
        TableScan rightScan = assertInstanceOf(TableScan.class, join.right(), "right Filter must be absorbed");
        assertSame(rightCondition, rightScan.predicate());
    }

    @Test
    void joinWithOnlyOneSideFiltered_otherSideStaysBare() {
        Predicate leftCondition = Predicate.compare("amount", ComparisonOp.GT, 10L);
        LogicalPlan plan = new Join(
                new Filter(new TableScan("orders", null, null), leftCondition),
                new TableScan("customers", null, null),
                List.of(new JoinKey("customer_id", "id")));

        LogicalPlan result = new PushFilterIntoScan().apply(plan);

        Join join = assertInstanceOf(Join.class, result);
        assertSame(leftCondition, ((TableScan) join.left()).predicate());
        assertNull(((TableScan) join.right()).predicate());
    }

    @Test
    void idempotent_onAlreadyOptimizedPlan() {
        Predicate condition = Predicate.compare("level", ComparisonOp.EQ, "ERROR");
        LogicalPlan already = new TableScan("logs", condition, null);

        assertSame(already, new PushFilterIntoScan().apply(already));
    }
}
